%% https://orientdb.com/docs/last/Record-Schemaless-Binary-Serialization.html
-module(odi_record_binary).

%% API
-export([encode_record/2, decode_record/1]).

encode_record(Class, Fields) ->
    {TmpHeader, HeaderPosReversed} = maps:fold(fun(K, {Type, _value}, {PrevHeader, PrevPos}) ->
        Start = encode([PrevHeader, {string, K}]),
        {encode([Start, <<0:32/integer>>, {byte, encode_type(Type)}]), [byte_size(Start) | PrevPos]}
    end, {<<>>, []}, Fields),
    {Data, DataPosReversed} = maps:fold(fun(_k, {Type, Value}, {PrevData, PrevPos}) ->
        {encode([PrevData, {Type, Value}]), [byte_size(PrevData) | PrevPos]}
    end, {<<>>, []}, Fields),
    Start = encode([{byte, 0}, {string, Class}]),
    DataOffset = byte_size(Start) + byte_size(TmpHeader) + 1,
    Header = lists:foldr(fun({PosHeader, PosData}, PrevHeader) ->
        <<Before:PosHeader/binary, 0:32/integer, After/binary>> = PrevHeader,
        <<Before/binary, (PosData + DataOffset):32/integer, After/binary>>
    end, TmpHeader, lists:zip(HeaderPosReversed, DataPosReversed)),
    encode([Start, Header, {byte, 0}, Data]).

decode_record(Bin) ->
    {{0, Class}, BinHeaders} = decode([byte, string], Bin),
    {HeadersReversed, BinData} = decode_headers(BinHeaders, []),
    {MinRestData, Data} = lists:foldr(fun({FieldName, CurDataOffset, DataType}, {PrevMinRestData, PrevData}) ->
        <<_Before:CurDataOffset/binary, CurData/binary>> = Bin,
        {Value, RestData} = decode(DataType, CurData),
        {min(PrevMinRestData, byte_size(RestData)), maps:put(FieldName, {DataType, Value}, PrevData)}
    end, {byte_size(BinData), #{}}, HeadersReversed),
    {Class, Data, binary_part(BinData, byte_size(BinData), -MinRestData)}.

decode_headers(<<0:8, Rest/binary>>, PrevHeaders) ->
    {PrevHeaders, Rest};
decode_headers(Header, PrevHeaders) ->
    {{FieldName, DataOffset, DataType}, NextHeader} = decode([string, int32, byte], Header),
    decode_headers(NextHeader, [{FieldName, DataOffset, decode_type(DataType)} | PrevHeaders]).

encode(B) when is_binary(B) ->
    B;
encode({byte, N}) ->
    <<N:8/integer>>;
encode({bool, false}) ->
    encode({byte, 0});
encode({bool, true}) ->
    encode({byte, 1});
encode({varint, N}) ->
    small_ints:encode_zigzag_varint(N);
encode({short, N}) ->
    encode({varint, N});
encode({integer, N}) ->
    encode({varint, N});
encode({long, N}) ->
    encode({varint, N});
encode({datetime, N}) ->
    encode({varint, N});
encode({date, N}) ->
    encode({varint, N});
encode({float, N}) ->
    <<N:32/float>>;
encode({double, N}) ->
    <<N:64/float>>;
encode({string, S}) when is_atom(S) ->
    encode({string, atom_to_list(S)});
encode({string, S}) ->
    <<(encode([{varint, length(S)}]))/binary, (list_to_binary(S))/binary>>;
encode({binary, S}) ->
    <<(encode([{varint, byte_size(S)}]))/binary, S/binary>>;
%TODO: embedded, embedded_list, embedded_set, embedded_map, decimal
encode({link, {ClusterId, RecordPosition}}) ->
    encode([{varint, ClusterId}, {varint, RecordPosition}]);
encode({link_list, Links}) ->
    LinksBin = << <<(encode({link, Link}))/binary>> || Link <- Links >>,
    encode([{varint, length(Links)}, LinksBin]);
encode({link_set, Links}) ->
    encode({link_list, Links});
encode({link_map, Links}) ->  %% TODO: only strings
    LinksBin = maps:fold(fun(K, V, Prev) ->
        encode([Prev, {byte, encode_type(string)}, {string, K}, {link, V}])
    end, <<>>, Links),
    encode([{varint, maps:size(Links)}, LinksBin]);
encode({linkbag, Links}) ->
    LinksBin = << <<ClusterId:16, RecordPosition:64>> || {ClusterId, RecordPosition} <- Links >>,
    <<1:8, (length(Links)):32, LinksBin/binary>>;
encode(L) when is_list(L) ->
    list_to_binary(lists:map(fun encode/1, L)).

decode(byte, <<N:8, Rest/binary>>) ->
    {N, Rest};
decode(int32, <<N:32, Rest/binary>>) ->
    {N, Rest};
decode(bool, <<0:8, Rest/binary>>) ->
    {false, Rest};
decode(bool, <<1:8, Rest/binary>>) ->
    {true, Rest};
decode(varint, Bin) ->
    small_ints:decode_zigzag_varint(Bin);
decode(short, Bin) ->
    decode(varint, Bin);
decode(integer, Bin) ->
    decode(varint, Bin);
decode(long, Bin) ->
    decode(varint, Bin);
decode(datetime, Bin) ->
    decode(varint, Bin);
decode(date, Bin) ->
    decode(varint, Bin);
decode(float, <<N:32/float, Rest/binary>>) ->
    {N, Rest};
decode(double, <<N:64/float, Rest/binary>>) ->
    {N, Rest};
decode(string, Bin) ->
    {Len, Rest} = decode(varint, Bin),
    <<Value:Len/binary, Rest2/binary>> = Rest,
    {binary_to_list(Value), Rest2};
decode(binary, Bin) ->
    {Len, Rest} = decode(varint, Bin),
    <<Value:Len/binary, Rest2/binary>> = Rest,
    {Value, Rest2};
decode(link, Bin) ->
    decode([varint, varint], Bin);
decode(link_list, Bin) ->
    {Len, Rest} = decode(varint, Bin),
    decode_link_list(Len, Rest, []);
decode(link_set, Bin) ->
    decode(link_list, Bin);
decode(link_map, Bin) ->
    {Len, Rest} = decode(varint, Bin),
    decode_link_map(Len, Rest, #{});
decode(linkbag, <<1:8, Len:32, Links/binary>>) ->
    decode_rid_list(Len, Links, []);
decode(L, Bin) when is_list(L) ->
    {Values, Rest} = decode_tuple(L, Bin),
    {list_to_tuple(Values), Rest}.

decode_tuple([], Bin) ->
    {[], Bin};
decode_tuple([Type | RestType], Bin) ->
    {Value, RestBin} = decode(Type, Bin),
    {RestValues, RestBins} = decode_tuple(RestType, RestBin),
    {[Value | RestValues], RestBins}.

decode_link_list(0, Bin, List) ->
    {lists:reverse(List), Bin};
decode_link_list(Remains, Bin, List) ->
    {{ClusterId, RecordPosition}, Rest} = decode([varint, varint], Bin),
    decode_link_list(Remains - 1, Rest, [{ClusterId, RecordPosition} | List]).

decode_rid_list(0, Bin, List) ->
    {lists:reverse(List), Bin};
decode_rid_list(Remains, <<ClusterId:16, RecordPosition:64, Rest/binary>>, List) ->
    decode_rid_list(Remains - 1, Rest, [{ClusterId, RecordPosition} | List]).

decode_link_map(0, Bin, Map) ->
    {Map, Bin};
decode_link_map(Remains, Bin, Map) ->
    {{7, Key, Link}, Rest} = decode([byte, string, link], Bin),
    decode_link_map(Remains - 1, Rest, Map#{Key => Link}).

encode_type(bool) -> 0;
encode_type(integer) -> 1;
encode_type(short) -> 2;
encode_type(long) -> 3;
encode_type(float) -> 4;
encode_type(double) -> 5;
encode_type(datetime) -> 6;
encode_type(string) -> 7;
encode_type(binary) -> 8;
encode_type(embedded) -> 9;
encode_type(embedded_list) -> 10;
encode_type(embedded_set) -> 11;
encode_type(embedded_map) -> 12;
encode_type(link) -> 13;
encode_type(link_list) -> 14;
encode_type(link_set) -> 15;
encode_type(link_map) -> 16;
encode_type(byte) -> 17;
encode_type(transient) -> 18;
encode_type(date) -> 19;
encode_type(custom) -> 20;
encode_type(decimal) -> 21;
encode_type(linkbag) -> 22;
encode_type(any) -> 23.

decode_type(0) -> bool;
decode_type(1) -> integer;
decode_type(2) -> short;
decode_type(3) -> long;
decode_type(4) -> float;
decode_type(5) -> double;
decode_type(6) -> datetime;
decode_type(7) -> string;
decode_type(8) -> binary;
decode_type(9) -> embedded;
decode_type(10) -> embedded_list;
decode_type(11) -> embedded_set;
decode_type(12) -> embedded_map;
decode_type(13) -> link;
decode_type(14) -> link_list;
decode_type(15) -> link_set;
decode_type(16) -> link_map;
decode_type(17) -> byte;
decode_type(18) -> transient;
decode_type(19) -> date;
decode_type(20) -> custom;
decode_type(21) -> decimal;
decode_type(22) -> linkbag;
decode_type(23) -> any.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% ODocument doc = new ODocument("V");
%% doc.field("toto", 12);
%% doc.field("tutu", "tutu");
%% doc.save(true);

%% 1f    O_RECORD_CREATE
%% 00000002
%% 0000   (cluster-id:short)
%% 007500000000047465737400ffffffffffffffffffff0000015d9e5c4fbf010004726f6f74002400174f5265636f726453657269616c697a657242696e617279000d4f7269656e744442204a6176610006322e322e323406230201b417aa256de3146654e264fc5c9e947ed2f92061f8f6725f95d93f49ffff0000001e
%% 00     (serialization-version:byte)
%% 0256   (class-name:string)
%% 08746f746f 00000018 01   (field-name:string)(pointer-to-data-structure:int32)(data-type:byte)
%% 0874757475 00000019 07   (field-name:string)(pointer-to-data-structure:int32)(data-type:byte)
%% 00   (header end)
%% 18   (data 1 varint)
%% 0874757475 (data 2 string)
%% 64 00  (record-type:byte)=d (mode:byte)=sync

encode_record_test() ->
    Expected = hex:hexstr_to_bin("00025608746f746f00000018010874757475000000190700180874757475"),
    Expected = encode_record("V", #{toto => {integer, 12}, tutu => {string, "tutu"}}).

decode_record_test() ->
    Bin = hex:hexstr_to_bin("00025608746f746f000000180108747574750000001907001808747574750102"),
    {Class, Data, Rest} = decode_record(Bin),
    "V" = Class,
    #{"toto" := {integer, 12}, "tutu" := {string, "tutu"}} = Data,
    ExpectedRest = hex:hexstr_to_bin("0102"),
    ExpectedRest = Rest.

link_list_test() ->
    List = [{1, 2}, {2, 3}],
    Bin = encode({link_list, List}),
    {List, <<>>} = decode(link_list, Bin).

link_map_test() ->
    Map = #{"a" => {1, 2}, "b" => {1,3}},
    Bin = encode({link_map, Map}),
    {Map, <<>>} = decode(link_map, Bin).

linkbag_test() ->
    Links = [{1, 2}, {3, 4}],
    Bin = encode({linkbag, Links}),
    {Links, <<>>} = decode(linkbag, Bin).

-endif.
