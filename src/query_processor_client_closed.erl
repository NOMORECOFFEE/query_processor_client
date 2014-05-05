-module(query_processor_client_closed).

-behaviour(gen_event).

-export([
    init/1,
    handle_event/2,
    terminate/2
]).

init([CollectionId]) ->
    {ok, CollectionId}.
    
handle_event({request, Packet}, CollectionId) ->
    gen_event:notify(self(), {request, Packet}),
    {swap_handler, normal, CollectionId, query_processor_client_connected, CollectionId}.


terminate(_, StateIn) ->
    ok
