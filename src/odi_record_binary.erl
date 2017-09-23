%% https://orientdb.com/docs/last/Record-Schemaless-Binary-Serialization.html
%% https://github.com/orientechnologies/orientdb/blob/master/core/src/main/java/com/orientechnologies/orient/core/serialization/serializer/record/binary/ORecordSerializerNetworkV0.java
-module(odi_record_binary).

%% API
-export([encode_record/3, decode_record/4, decode_type/1]).

-include("../include/odi.hrl").

encode_record(Class, Fields, Offset) ->
    {Start, OffsetHeader} = encode([{byte, 0}, {string, Class}], Offset),
    {TmpHeader, HeaderPosReversed} = maps:fold(fun(K, {Type, _value}, {PrevHeader, PrevPos}) ->
        {HeaderStart, _} = encode([PrevHeader, {string, K}], OffsetHeader),
        {<<HeaderStart/binary, 0:32, (encode_type(Type)):8>>, [byte_size(HeaderStart) | PrevPos]}
    end, {<<>>, []}, Fields),
    lager:debug("TmpHeaders=~p, HeaderPosReversed=~p", [TmpHeader, HeaderPosReversed]),
    DataOffset = OffsetHeader + byte_size(TmpHeader) + 1,
    {Data, DataPosReversed} = maps:fold(fun(_k, {Type, Value}, {PrevData, PrevPos}) ->
        case Value of
            null ->
                {PrevData, [null | PrevPos]};
            _ ->
                {CurData, _} = encode([PrevData, {Type, Value}], DataOffset),
                {CurData, [byte_size(PrevData) | PrevPos]}
        end
    end, {<<>>, []}, Fields),
    Header = lists:foldr(fun({PosHeader, PosData}, PrevHeader) ->
        case PosData of
            null ->
                PrevHeader;
            _ ->
                <<Before:PosHeader/binary, 0:32/integer, After/binary>> = PrevHeader,
                <<Before/binary, (PosData + DataOffset):32/integer, After/binary>>
        end
    end, TmpHeader, lists:zip(HeaderPosReversed, DataPosReversed)),
    {<<Start/binary, Header/binary, 0:8, Data/binary>>, DataOffset + byte_size(Data)}.

decode_record($d, Bin, GlobalBin, GlobalProperties) ->
    {{0, Class}, BinHeaders} = decode([byte, string], Bin, GlobalBin),
    lager:debug("decode_record class=~s", [Class]),
    {HeadersReversed, BinData} = decode_headers(BinHeaders, [], GlobalBin, GlobalProperties),
    {MinRestData, Data} = decode_data(GlobalBin, BinData, HeadersReversed),
    lager:debug("decode_record MinRestData=~p: ~p",[MinRestData, Data]),
    {Class, Data, binary_part(BinData, byte_size(BinData), -MinRestData)};
decode_record($b, Bin, _GlobalBin, _GlobalProperties) ->
    {raw, Bin, <<>>}.

decode_data(GlobalBin, BinData, HeadersReversed) ->
    {MinRestData, Data} = lists:foldr(
        fun({FieldName, CurDataOffset, DataType}, {PrevMinRestData, PrevData}) ->
            case CurDataOffset of
                0 ->
                    lager:debug("Decode null field ~s (~p)", [FieldName, DataType]),
                    {PrevMinRestData, maps:put(FieldName, {DataType, null}, PrevData)};
                _ ->
                    <<_Before:CurDataOffset/binary, CurData/binary>> = GlobalBin,
                    lager:debug("Decode field ~p (~p) +~p", [FieldName, DataType, CurDataOffset]),
                    {Value, RestData} = decode(DataType, CurData, GlobalBin),
                    {min(PrevMinRestData, byte_size(RestData)), maps:put(FieldName, {DataType, Value},
                        PrevData)}
            end
        end, {byte_size(BinData), #{}}, HeadersReversed),
    {MinRestData, Data}.

decode_headers(<<0:8, Rest/binary>>, PrevHeaders, _GlobalBin, _GlobalProperties) ->
    lager:debug("no more headers", []),
    {PrevHeaders, Rest};
decode_headers(Header, PrevHeaders, GlobalBin, GlobalProperties) ->
    {Len, _Rest} = decode(varint, Header, GlobalBin),
    case Len >= 0 of
        true ->
            {{FieldName, DataOffset, DataType}, NextHeader} = decode([string, int32, byte], Header, GlobalBin),
            lager:debug("new named field ~s ~p", [FieldName, decode_type(DataType)]),
            decode_headers(NextHeader, [{FieldName, DataOffset, decode_type(DataType)} | PrevHeaders], GlobalBin, GlobalProperties);
        false ->
            {{PropertyId, DataOffset}, NextHeader} = decode([varint, int32], Header, GlobalBin),
            FixedPropertyId = (PropertyId * -1) - 1,
            lager:debug("new property ~p", [FixedPropertyId]),
            #{FixedPropertyId := PropertyDef} = GlobalProperties,
            #{"name" := Name, "type" := Type} = PropertyDef,
            decode_headers(NextHeader, [{Name, DataOffset, convert_type(Type)} | PrevHeaders], GlobalBin, GlobalProperties)
    end.

encode(B, Offset) when is_binary(B) ->
    {B, Offset + byte_size(B)};
encode({byte, N}, Offset) ->
    {<<N:?o_byte>>, Offset + 1};
encode({bool, false}, Offset) ->
    encode({byte, 0}, Offset);
encode({bool, true}, Offset) ->
    encode({byte, 1}, Offset);
encode({varint, N}, Offset) ->
    Bin = small_ints:encode_zigzag_varint(N),
    {Bin, Offset + byte_size(Bin)};
encode({short, N}, Offset) ->
    encode({varint, N}, Offset);
encode({integer, N}, Offset) ->
    encode({varint, N}, Offset);
encode({long, N}, Offset) ->
    encode({varint, N}, Offset);
encode({datetime, N}, Offset) ->
    encode({varint, N}, Offset);
encode({date, N}, Offset) ->
    encode({varint, N}, Offset);
encode({float, N}, Offset) ->
    {<<N:?o_float>>, Offset + 4};
encode({double, N}, Offset) ->
    {<<N:?o_double>>, Offset + 8};
encode({string, S}, Offset) when is_atom(S) ->
    encode({string, atom_to_list(S)}, Offset);
encode({string, S}, Offset) ->
    Flat = lists:flatten(S),
    encode([{varint, length(Flat)}, list_to_binary(Flat)], Offset);
encode({binary, S}, Offset) ->
    encode([{varint, byte_size(S)}, S], Offset);
encode({embedded, {Class, Data}}, Offset) ->
    {Bin, NewOffset} = encode_record(Class, Data, Offset - 1),
    <<0:8, BinRecord/binary>> = Bin,
    {BinRecord, NewOffset};
encode({embedded_list, L}, Offset) ->
    {Header, ListOffset} = encode([{varint, length(L)}, {byte, encode_type(any)}], Offset),
    Body = lists:foldl(fun(I, Prev) ->
        {New, _} = encode([Prev, {any, I}], ListOffset),
        New
    end, <<>>, L),
    {<<Header/binary, Body/binary>>, ListOffset + byte_size(Body)};
encode({embedded_set, L}, Offset) ->
    encode({embedded_list, L}, Offset);
encode({embedded_map, Map}, Offset) ->  %% TODO: only string keys
    {Start, OffsetHeader} = encode([{varint, map_size(Map)}], Offset),
    {TmpHeader, HeaderPosReversed} = maps:fold(fun(K, {Type, _value}, {PrevHeader, PrevPos}) ->
        {HeaderStart, _} = encode([PrevHeader, {byte, encode_type(string)}, {string, K}], OffsetHeader),
        {<<HeaderStart/binary, 0:32, (encode_type(Type)):8>>, [byte_size(HeaderStart) | PrevPos]}
    end, {<<>>, []}, Map),
    DataOffset = OffsetHeader + byte_size(TmpHeader),
    {Data, DataPosReversed} = maps:fold(fun(_k, {Type, Value}, {PrevData, PrevPos}) ->
        case Value of
            null ->
                {PrevData, [null | PrevPos]};
            _ ->
                {CurData, _} = encode([PrevData, {Type, Value}], DataOffset),
                {CurData, [byte_size(PrevData) | PrevPos]}
        end
    end, {<<>>, []}, Map),
    Header = lists:foldr(fun({PosHeader, PosData}, PrevHeader) ->
        case PosData of
            null ->
                PrevHeader;
            _ ->
                <<Before:PosHeader/binary, 0:32/integer, After/binary>> = PrevHeader,
                <<Before/binary, (PosData + DataOffset):32/integer, After/binary>>
        end
    end, TmpHeader, lists:zip(HeaderPosReversed, DataPosReversed)),
    {<<Start/binary, Header/binary, Data/binary>>, DataOffset + byte_size(Data)};
%TODO: decimal
encode({link, {ClusterId, RecordPosition}}, Offset) ->
    encode([{varint, ClusterId}, {varint, RecordPosition}], Offset);
encode({link_list, Links}, Offset) ->
    encode([{varint, length(Links)} | [{link, Link} || Link <- Links]], Offset);
encode({link_set, Links}, Offset) ->
    encode({link_list, Links}, Offset);
encode({link_map, Links}, Offset) ->  %% TODO: only string keys
    LinksBin = maps:fold(fun(K, V, Prev) ->
        {CurLink, _} = encode([Prev, {byte, encode_type(string)}, {string, K}, {link, V}], Offset),
        CurLink
    end, <<>>, Links),
    encode([{varint, maps:size(Links)}, LinksBin], Offset);
encode({linkbag, {Uuid, Links}}, Offset) ->
    LinksBin = << <<ClusterId:?o_short, RecordPosition:?o_long>> || {ClusterId, RecordPosition} <- Links >>,
    Bin = <<3:8, Uuid:128, (length(Links)):?o_int, LinksBin/binary>>,
    {Bin, Offset + byte_size(Bin)};
encode({linkbag, Links}, Offset) ->
    LinksBin = << <<ClusterId:?o_short, RecordPosition:?o_long>> || {ClusterId, RecordPosition} <- Links >>,
    Bin = <<1:8, (length(Links)):?o_int, LinksBin/binary>>,
    {Bin, Offset + byte_size(Bin)};
encode({any, {Type, Value}}, Offset) ->
    {Bin, NewOffset} = encode({Type, Value}, Offset + 1),
    {<<(encode_type(Type)):8, Bin/binary>>, NewOffset};
encode(L, Offset) when is_list(L) ->
    lists:foldl(fun(I, {Acc, PrevOffset}) ->
        {Cur, NextOffset} = encode(I, PrevOffset),
        {<<Acc/binary, Cur/binary>>, NextOffset}
    end, {<<>>, Offset}, L).

decode(byte, <<N:?o_byte, Rest/binary>>, _GlobalBin) ->
    lager:debug("decode(byte, ~p)", [N]),
    {N, Rest};
decode(int32, <<N:?o_int, Rest/binary>>, _GlobalBin) ->
    lager:debug("decode(int32, ~p)", [N]),
    {N, Rest};
decode(bool, <<0:8, Rest/binary>>, _GlobalBin) ->
    lager:debug("decode(bool, false)", []),
    {false, Rest};
decode(bool, <<1:8, Rest/binary>>, _GlobalBin) ->
    lager:debug("decode(bool, true)", []),
    {true, Rest};
decode(varint, Bin, _GlobalBin) ->
    {N, Rest} =small_ints:decode_zigzag_varint(Bin),
    lager:debug("decode(varint, ~p)", [N]),
    {N, Rest};
decode(short, Bin, GlobalBin) ->
    decode(varint, Bin, GlobalBin);
decode(integer, Bin, GlobalBin) ->
    decode(varint, Bin, GlobalBin);
decode(long, Bin, GlobalBin) ->
    decode(varint, Bin, GlobalBin);
decode(datetime, Bin, GlobalBin) ->
    decode(varint, Bin, GlobalBin);
decode(date, Bin, GlobalBin) ->
    decode(varint, Bin, GlobalBin);
decode(float, <<N:?o_float, Rest/binary>>, _GlobalBin) ->
    lager:debug("decode(float)", []),
    {N, Rest};
decode(double, <<N:?o_double, Rest/binary>>, _GlobalBin) ->
    lager:debug("decode(double, ~p)", [N]),
    {N, Rest};
decode(string, Bin, GlobalBin) ->
    lager:debug("decode(string)", []),
    {Len, Rest} = decode(varint, Bin, GlobalBin),
    true = Len >= 0,
    <<Value:Len/binary, Rest2/binary>> = Rest,
    lager:debug("Reading string len=~p: ~s", [Len, Value]),
    {binary_to_list(Value), Rest2};
decode(binary, Bin, GlobalBin) ->
    lager:debug("decode(binary)", []),
    {Len, Rest} = decode(varint, Bin, GlobalBin),
    <<Value:Len/binary, Rest2/binary>> = Rest,
    {Value, Rest2};
decode(link, Bin, GlobalBin) ->
    lager:debug("decode(link)", []),
    decode([varint, varint], Bin, GlobalBin);
decode(link_list, Bin, GlobalBin) ->
    lager:debug("decode(link_list)", []),
    {Len, Rest} = decode(varint, Bin, GlobalBin),
    decode_link_list(Len, Rest, [], GlobalBin);
decode(link_set, Bin, GlobalBin) ->
    lager:debug("decode(link_set)", []),
    decode(link_list, Bin, GlobalBin);
decode(link_map, Bin, GlobalBin) ->
    lager:debug("decode(link_map)", []),
    {Len, Rest} = decode(varint, Bin, GlobalBin),
    decode_link_map(Len, Rest, #{}, GlobalBin);
decode(linkbag, <<0:6, 0:1, 1:1, Len:32, Links/binary>>, GlobalBin) ->
    lager:debug("decode(linkbag, embedded, unassigned)", []),
    decode_rid_list(Len, Links, [], GlobalBin);
decode(linkbag, <<0:6, 1:1, 1:1, Uuid:128, Len:?o_int, Links/binary>>, GlobalBin) ->
    lager:debug("decode(linkbag, embedded, assigned)", []),
    {List, Rest} = decode_rid_list(Len, Links, [], GlobalBin),
    {{Uuid, List}, Rest};
decode(embedded, Bin, GlobalBin) ->
    lager:debug("decode(embedded)", []),
    {Class, Data, Rest} = decode_record($d, <<0:8, Bin/binary>>, GlobalBin, #{}),
    {{Class, Data}, Rest};
decode(embedded_list, Bin, GlobalBin) ->
    {{Num, Type}, List} = decode([varint, byte], Bin, GlobalBin),
    lager:debug("decode(embedded_list, ~p, ~p)", [Num, decode_type(Type)]),
    decode_list(Num, decode_type(Type), List, [], GlobalBin);
decode(embedded_set, Bin, GlobalBin) ->
    decode(embedded_list, Bin, GlobalBin);
decode(embedded_map, Bin, GlobalBin) ->
    {Size, HeadersBin} = decode(varint, Bin, GlobalBin),
    {HeadersReversed, BinData} = decode_embedded_map_headers(Size, HeadersBin, GlobalBin, []),
    {MinRestData, Data} = decode_data(GlobalBin, BinData, HeadersReversed),
    lager:debug("decode_record MinRestData=~p: ~p",[MinRestData, Data]),
    {Data, binary_part(BinData, byte_size(BinData), -MinRestData)};
decode(any, <<Type:8, Bin/binary>>, GlobalBin) ->
    DecodedType = decode_type(Type),
    lager:debug("decode(any, ~p)", [DecodedType]),
    {Value, Rest} = decode(DecodedType, Bin, GlobalBin),
    {{DecodedType, Value}, Rest};
decode(L, Bin, GlobalBin) when is_list(L) ->
    lager:debug("decode(list)", []),
    {Values, Rest} = decode_tuple(L, Bin, GlobalBin),
    {list_to_tuple(Values), Rest}.

decode_list(0, _Type, Rest, Acc, _GlobalBin) ->
    {lists:reverse(Acc), Rest};
decode_list(Remains, Type, Bin, Acc, GlobalBin) ->
    {Item, Rest} = decode(Type, Bin, GlobalBin),
    decode_list(Remains - 1, Type, Rest, [Item | Acc], GlobalBin).

decode_tuple([], Bin, _GlobalBin) ->
    {[], Bin};
decode_tuple([Type | RestType], Bin, GlobalBin) ->
    {Value, RestBin} = decode(Type, Bin, GlobalBin),
    {RestValues, RestBins} = decode_tuple(RestType, RestBin, GlobalBin),
    {[Value | RestValues], RestBins}.

decode_link_list(0, Bin, List, _GlobalBin) ->
    {lists:reverse(List), Bin};
decode_link_list(Remains, Bin, List, GlobalBin) ->
    {{ClusterId, RecordPosition}, Rest} = decode([varint, varint], Bin, GlobalBin),
    decode_link_list(Remains - 1, Rest, [{ClusterId, RecordPosition} | List], GlobalBin).

decode_rid_list(0, Bin, List, _GlobalBin) ->
    {lists:reverse(List), Bin};
decode_rid_list(Remains, <<ClusterId:?o_short, RecordPosition:?o_long, Rest/binary>>, List, GlobalBin) ->
    decode_rid_list(Remains - 1, Rest, [{ClusterId, RecordPosition} | List], GlobalBin).

decode_link_map(0, Bin, Map, _GlobalBin) ->
    {Map, Bin};
decode_link_map(Remains, Bin, Map, GlobalBin) ->
    {{7, Key, Link}, Rest} = decode([byte, string, link], Bin, GlobalBin),
    decode_link_map(Remains - 1, Rest, Map#{Key => Link}, GlobalBin).

decode_embedded_map_headers(0, Bin, _GlobalBin, List) ->
    {List, Bin};
decode_embedded_map_headers(Size, <<KeyTypeBin:?o_byte, Bin/binary>>, GlobalBin, List) ->
    KeyType = decode_type(KeyTypeBin),
    {{Key, DataPointer, ValueType}, Rest} = decode([KeyType, int32, byte], Bin, GlobalBin),
    decode_embedded_map_headers(Size - 1, Rest, GlobalBin,
                                [{Key, DataPointer, decode_type(ValueType)} | List]).

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

convert_type("BOOLEAN") ->
    bool;
convert_type("EMBEDDEDMAP") ->
    embedded_map;
convert_type("EMBEDDEDLIST") ->
    embedded_list;
convert_type("EMBEDDEDSET") ->
    embedded_set;
convert_type(Type) ->
    list_to_atom(string:lowercase(Type)).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

encode_record_test() ->
    Expected = hex:hexstr_to_bin("00025608746f746f00000018010874757475000000190700180874757475"),
    Offset = byte_size(Expected),
    {Expected, Offset} = encode_record("V", #{toto => {integer, 12}, tutu => {string, "tutu"}}, 0).

decode_record_test() ->
    Bin = hex:hexstr_to_bin("00025608746f746f000000180108747574750000001907001808747574750102"),
    {Class, Data, Rest} = decode_record($d, Bin, Bin, #{}),
    "V" = Class,
    #{"toto" := {integer, 12}, "tutu" := {string, "tutu"}} = Data,
    ExpectedRest = hex:hexstr_to_bin("0102"),
    ExpectedRest = Rest.

link_list_test() ->
    List = [{1, 2}, {2, 3}],
    {Bin, Offset} = encode({link_list, List}, 0),
    Offset = byte_size(Bin),
    {List, <<>>} = decode(link_list, Bin, Bin).

link_map_test() ->
    Map = #{"a" => {1, 2}, "b" => {1,3}},
    {Bin, Offset} = encode({link_map, Map}, 0),
    Offset = byte_size(Bin),
    {Map, <<>>} = decode(link_map, Bin, Bin).

linkbag_test() ->
    Links = [{1, 2}, {3, 4}],
    {Bin, Offset} = encode({linkbag, Links}, 0),
    Offset = byte_size(Bin),
    {Links, <<>>} = decode(linkbag, Bin, Bin).

empty_embedded_set_test() ->
    {Bin, Offset} = encode({embedded_set, []}, 0),
    Offset = byte_size(Bin),
    {[], <<>>} = decode(embedded_set, Bin, Bin).


complex_test() ->
    Data = #{
        "blobClusters" => {embedded_set,[]},
        "classes" => {embedded_set,[
            {embedded , {[],
                #{"abstract" => {bool,false},
                    "clusterIds" => {embedded_list,[{integer, 6}]},
                    "clusterSelection" => {string,"round-robin"},
                    "customFields" => {bool,null},
                    "defaultClusterId" => {integer,6},
                    "description" => {bool,null},
                    "name" => {string,"OFunction"},
                    "overSize" => {float,0.0},
                    "properties" => {embedded_set,[
                        {embedded , {[],
                            #{
                                "collate" => {string,"default"},
                                "customFields" => {bool,null},
                                "defaultValue" => {bool,null},
                                "description" => {bool,null},
                                "globalId" => {integer,11},
                                "mandatory" => {bool,false},
                                "max" => {bool,null},
                                "min" => {bool,null},
                                "name" => {string,"code"},
                                "notNull" => {bool,false},
                                "readonly" => {bool,false},
                                "type" => {integer,7}}}},
                        {embedded , {[],
                            #{
                                "collate" => {string,"default"},
                                "customFields" => {bool,null},
                                "defaultValue" => {bool,null},
                                "description" => {bool,null},
                                "globalId" => {integer,0},
                                "mandatory" => {bool,true},
                                "max" => {bool,null},
                                "min" => {bool,null},
                                "name" => {string,"name"},
                                "notNull" => {bool,true},
                                "readonly" => {bool,false},
                                "type" => {integer,7}}}}
                    ]},
                    "shortName" => {bool,null},
                    "strictMode" => {bool,false},
                    "superClass" => {bool,null},
                    "superClasses" => {bool,null}}}},
            {embedded ,
                {[],
                    #{
                        "abstract" => {bool,false},
                        "clusterIds" => {embedded_list,[{integer, 9},{integer, 10},{integer, 11},{integer, 12},{integer, 13},
                                                        {integer, 14},{integer, 15},{integer, 16}]},
                        "clusterSelection" => {string,"round-robin"},
                        "customFields" => {bool,null},
                        "defaultClusterId" => {integer,9},
                        "description" => {bool,null},
                        "name" => {string,"V"},
                        "overSize" => {float,0.0},
                        "properties" => {embedded_set,[]},
                        "shortName" => {bool,null},
                        "strictMode" => {bool,false},
                        "superClass" => {bool,null},
                        "superClasses" => {bool,null}}}}
        ]},
        "globalProperties" => {embedded_list,[
            {embedded, {[],
                #{"id" => {integer,0},
                    "name" => {string,"name"},
                    "type" => {string,"STRING"}}}},
            {embedded, {[],
                #{"id" => {integer,1},
                    "name" => {string,"mode"},
                    "type" => {string,"BYTE"}}}}
        ]},
        "schemaVersion" => {integer,4}},
    {Bin, Offset} = encode_record("", Data, 0),
    Offset = byte_size(Bin),
    {"", Data, <<>>} = decode_record($d, Bin, Bin, #{}).

edge_test() ->
    Bin = hex:hexstr_to_bin("00025608746f746f000000220108747574750000002307086f75745f000000281600180874757475039533e8cb477648ab976dc8c38151667d00000001fffffffffffffffffffc"),
    Data = #{
        "out_" => {linkbag, {198324500136199210974642822101347690109, [{-1, -4}]}},
        "toto" => {integer, 12},
        "tutu" => {string, "tutu"}
    },
    {"V", Data, <<>>} = decode_record($d, Bin, Bin, #{}),
    BinLen = byte_size(Bin),
    io:format("Bin=~p~n", [Bin]),
    {Bin2, BinLen} = encode_record("V", Data, 0),  %% map order is not the same between java and erlang, so cannot compare Bin2 and Bin
    {"V", Data, <<>>} = decode_record($d, Bin2, Bin2, #{}).

embedded_map_test() ->
    Map = #{"int" => {integer, 12}, "string" => {string, "abcdef"}},
    {Data, Offset} = encode({embedded_map, Map}, 4),
    ExceptedOffset = 4 + byte_size(Data),
    ExceptedOffset = Offset,
    {Map, <<>>} = decode(embedded_map, Data, <<0:?o_int, Data/binary>>).

-endif.
