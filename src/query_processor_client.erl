-module(query_processor_client).

-behaviour(gen_fsm).

-export([closed/2, closed/3, connected/2, connected/3]).

-export([
    init/1,
    handle_event/3,
    handle_sync_event/4,
    handle_info/3,
    terminate/3,
    code_change/4
]).

-export(
   [
    start/1,
    request/2,
    check_request/1
   ]).

-record(state, {
          server                :: inet:ip_address() | inet:hostname(),
          port                  :: inet:port_number(),
          connection            :: gen_tcp:socket(),
          timeout = infinity    :: timeout(),    %% время, после которго сокет закроется,
                                                 %% если не поступит новых запросов
          requests = dict:new() :: dict()
         }).

-define(CONFIG, "query_processor_client.conf").

-define(DEFAULT_HOST, localhost).
-define(DEFAULT_PORT, 8840).

start(CollectionId) ->
    gen_fsm:start_link(?MODULE, CollectionId, []).

-spec request(CollectionId :: atom(), Request :: array()) -> {ok, Response :: array()} | {error, Reason :: term()}.
request(CollectionId, Packet) ->
    Client = gproc:where({n, l, {?MODULE, CollectionId}}),
    gen_fsm:sync_send_event(Client, {request, Packet}).

check_request({error, Reason}) ->
    {error, Reason};

check_request(Request) ->
    case proplists:lookup(error, Request) of
        {error, Reason} ->
            {error, Reason};
        none ->
            {ok, Request}
    end.

connected({request, Packet}, From, StateIn) ->
    do_send_request(Packet, From, StateIn);

connected(_Msg, _From, StateIn) ->
    ?LOG(?LERROR, "unknown message", [{message, _Msg}]),
    {next_state, connected, StateIn}.

connected(timeout, StateIn) ->
    gen_tcp:close(StateIn#state.connection),
    {next_state, closed, StateIn};

connected(_Msg, StateIn) ->
    ?LOG(?LERROR, "unknown message", [{message, _Msg}]),
    {next_state, connected, StateIn}.

closed({request, Packet}, From, #state{server = Hostname, port = Port} = StateIn) ->
    do_send_request(Packet, From, StateIn#state{connection = do_connect(Hostname, Port)}).

closed(_Msg, StateIn) ->
    ?LOG(?LERROR, "unknown message", [{message, _Msg}]),
    {next_state, closed, StateIn}.

init(CollectionId) ->
    kmsutil:set_name(?MODULE),
    kmsrestarter:startup_sleep(self(), ?MODULE),

    gen_fsm:send_all_state_event(self(), {init, CollectionId}),

    {ok, closed, #state{}}.

handle_sync_event(_Msg, _From, StateName, State) ->
    ?LOG(?LERROR, "unknown message", [{message, _Msg}]),
    {next_state, StateName, State}.


handle_event({init, CollectionId}, closed, StateIn) ->
    Options = do_get_options(CollectionId),
    Hostname = kmsutil:string_to_list(proplists:get_value(host, Options, ?DEFAULT_HOST)),
    Port = proplists:get_value(port, Options, ?DEFAULT_PORT),
    Timeout = proplists:get_value(timeout, Options, StateIn#state.timeout),
    true = gproc:reg({n, l, {?MODULE, CollectionId}}),
    State = StateIn#state{
        server = Hostname,
        port = Port,
        timeout = Timeout
    },
    case proplists:get_bool('on-demand', Options) of
        true ->
            {next_state, closed, State};
        false ->
            ?LOG(?LINFO, "connect query-processor client",
                [
                    {collection_id, CollectionId},
                    {server, Hostname},
                    {port, Port}
                ]),
            Connection = do_connect(Hostname, Port),
            {next_state, connected, State#state{connection = Connection}}
    end;

handle_event(_Msg, StateName, State) ->
    ?LOG(?LERROR, "unknown message", [{message, _Msg}]),
    {next_state, StateName, State}.


handle_info({tcp, Connection, <<1:8, Data/binary>>}, StateName, #state{connection = Connection} = StateIn) ->
    State = do_packet_handler(Data, StateIn),
    {next_state, StateName, State, do_timeout(State)};

handle_info({tcp_error, Connection, Reason}, _StateName, StateIn = #state{connection = Connection}) ->
    ?LOG(?LERROR, "tcp connection error",
                  [
                   {server, StateIn#state.server},
                   {port, StateIn#state.port},
                   {reason, Reason}
                  ]),
    {stop, connection_error, StateIn};

handle_info(_Msg, StateName, State) ->
    ?LOG(?LERROR, "unknown message", [{message, _Msg}]),
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

do_connect(Hostname, Port) ->
    {ok, Connection} = gen_tcp:connect(
        Hostname,
        Port,
        [binary, {packet, 4}, {active, true}, {reuseaddr, true}]),
    ?LOG(?LINFO, "query processor connected", []),
    Connection.

do_send_request(Packet, From, StateIn)  ->
    {Id, PacketBinary} = do_encode(Packet),
    ok = gen_tcp:send(StateIn#state.connection, <<1:8, PacketBinary/binary>>),
    {next_state, connected, StateIn#state{requests = dict:store(Id, From, StateIn#state.requests)}}.

do_decode(Data) ->
    Packet = binary_to_term(Data),
    Id = proplists:get_value('query-id', Packet),
    {Id, Packet}.

do_encode(Packet) ->
    Id = proplists:get_value('query-id', Packet),
    PacketBinary = term_to_binary(Packet),
    {Id, PacketBinary}.

do_fetch_client(Id, Requests) ->
    {dict:fetch(Id, Requests), dict:erase(Id, Requests)}.

do_timeout(#state{timeout = Timeout, requests = Requests}) ->
    case dict:is_empty(Requests) of
      true ->
          Timeout;
      false ->
          infinity
    end.

do_packet_handler(Data, #state{requests = RequestsIn} = StateIn) ->
    try
        {Id, Packet} = do_decode(Data),
        {ClientId,  Requests} = do_fetch_client(Id, RequestsIn),
        gen_fsm:reply(ClientId, Packet),
        StateIn#state{requests = Requests}
    catch _ : Reason ->
        ?LOG(?LERROR, "packet handler error", [{reason, Reason}]),
        StateIn
    end.

do_get_options(CollectionId) ->
    case kmsconfig:get_value(?CONFIG, [CollectionId, 'query-processing'], []) of
        [] ->
            ?LOG(?LERROR, "There is no query processing options", [{collection_id, CollectionId}]),
            kmsconfig:get_value(?CONFIG, ['default-query-processing'], []);
        Options ->
            Options
    end.

