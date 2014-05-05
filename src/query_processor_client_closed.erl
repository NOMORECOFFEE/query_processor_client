-module(query_processor_client_closed).

-behaviour(gen_event).

-export([
    init/1,
    handle_event/2,
    terminate/2
]).
    
handle_event({request, Packet}, #state{id = Id} = StateIn) ->
    gen_event:notify(self(), {request, Packet}),
    {swap_handler, normal, StateIn, query_processor_client_connected, Id}.

handle_call(_Event, StateIn) ->
    {ok, {error, _Event}, StateIn}.
    
terminate(_, StateIn) ->
    ok
