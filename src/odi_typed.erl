-module(odi_typed).

%% API
-export([typify_record/2, untypify_record/1]).

typify_record({Class, Data}, Classes) ->
    #{Class := ClassDef} = Classes,
    #{"strictMode" := StrictMode, "properties" := Properties} = ClassDef,
    DataTypified = maps:map(fun(K, V) ->
        typify_field(K, V, StrictMode, Properties, Classes)
                            end, Data),
    {Class, DataTypified}.

untypify_record({[], Data}) when is_map(Data) ->
    untypify_record(Data);
untypify_record({Class, Data}) when is_list(Class) and is_map(Data) ->
    {Class, untypify_record(Data)};
untypify_record(Map) when is_map(Map) ->
    maps:map(fun(_K, V) -> untypify_record(V) end, Map);
untypify_record({embedded, R}) ->
    untypify_record(R);
untypify_record({embedded_set, List}) ->
    lists:map(fun untypify_record/1, List);
untypify_record({embedded_list, List}) ->
    lists:map(fun untypify_record/1, List);
untypify_record({link_list, List}) ->
    lists:map(fun untypify_record/1, List);
untypify_record({link_set, List}) ->
    lists:map(fun untypify_record/1, List);
untypify_record({linkbag, List}) ->
    lists:map(fun untypify_record/1, List);
untypify_record({link_map, Map}) ->
    maps:map(fun(_K, V) -> untypify_record(V) end, Map);
untypify_record({ClusterId, RecordPosition}=R) when is_integer(ClusterId) and is_integer(RecordPosition) ->
    R;
untypify_record({Type, Value}) when is_atom(Type)->
    Value.

typify_field(K, V, Strict, Properties, Classes) ->
    case {Properties, Strict} of
        {#{K := Property}, _} ->
            typify_known_field(V, Property, Classes);
        {_, false} ->
            typify_unknown_field(V)

    end.

%% TODO: embedded_list, embedded_set, embedded_map, linkbag
typify_known_field(V, #{"type" := 9}, Classes) ->
    {embedded, typify_record({[], V}, Classes)};
typify_known_field(V, #{"type" := Type}, _Classes) ->
    {odi_record_binary:decode_type(Type), V}.


%% TODO: more types
typify_unknown_field(V) when is_integer(V), V =< 2147483647, V >= -2147483648 ->
    {integer, V};
typify_unknown_field(V) when is_integer(V), V =< 9223372036854775807, V >=  -9223372036854775808 ->
    {long, V};
typify_unknown_field(V) when is_list(V) ->
    %% TODO: detect if it's not a link_list or embedded_list
    {string, V};
typify_unknown_field(V) when is_float(V) ->
    {double, V}.
