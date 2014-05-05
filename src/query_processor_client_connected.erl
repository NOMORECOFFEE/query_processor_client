-module(query_processor_client_connected).

-behaviour(gen_event).

-export([
    init/1,
    handle_event/2,
    terminate/2,
    handle_info/2
]).

-define(TCP_OPTIONS, [binary, {packet, 4}, {active, true}, {reuseaddr, true}]).

init([collectionId]) ->
    {Host, Port} = do_get_options(collectionId),
    gen_tcp:connect(Hostname, Port, ?TCP_OPTIONS).

init({Args, ok}) ->
    init(Args).
    
handle_event({request, Packet}, Connection) ->
    {Id, PacketBinary} = do_encode(Packet),
    ok = gen_tcp:send(Connection, <<1:8, PacketBinary/binary>>),
    {ok, Connection}.

handle_info({tcp, Connection, <<1:8, Data/binary>>}, Connection) ->
    State = do_packet_handler(Data, Connection),
    {ok, State};
    
terminate(_, StateIn) ->
    ok
