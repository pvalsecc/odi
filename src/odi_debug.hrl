%-define(odi_debug(Txt, Args), io:format(Txt, Args)).
-define(odi_debug(_Txt, _Args), ok).

-define(odi_debug_graph(Txt, Args), io:format(Txt, Args)).
