-define(odi_debug_sock(Txt, Args), io:format(Txt, Args)).
%-define(odi_debug_sock(_Txt, _Args), ok).

-define(odi_debug_graph(Txt, Args), io:format(Txt, Args)).
%-define(odi_debug_graph(Txt, Args), ok).

%-define(odi_debug_record(Txt, Args), io:format(Txt, Args)).
-define(odi_debug_record(Txt, Args), ok).
