%%%-------------------------------------------------------------------
%% @copyright Geoff Cant
%% @author Geoff Cant <nem@erlang.geek.nz>
%% @version {@vsn}, {@date} {@time}
%% @doc Attempts to implement the 'Asynchronous Bully Algorithm' from
%% Scott D. Stoller's 1997 paper 'Leader Election in Distributed
%% Systems with Crash Failures' [http://www.cs.sunysb.edu/~stoller/leader-election.html]
%%
%% Notes:
%%   Our failure detector is built on erlang:monitor_node
%% @end
%%%-------------------------------------------------------------------
-module(gl_async_bully).

-behaviour(gen_fsm).

-include("ng_log.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/1
         ]).

%% Election states
-export([recovery/2,
         norm/2,
         elec2/2,
         wait/2]).

%% gen_fsm callbacks
-export([init/1, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-type incarnation() :: {non_neg_integer(),
                        non_neg_integer(),
                        non_neg_integer()}.

-type election_num() :: non_neg_integer().

-type election_id() :: {Node::atom(),
                        incarnation(),
                        election_num()}.

-record(state, {leader = node() :: atom(),
                elid :: election_id(),
                acks = ordsets:new() :: ordsets:ordset(node()),
                nextel = 0 :: election_num(),
                pendack :: node(),
                incarn :: incarnation(),
                %% Modifications
                peers = ordsets:new() :: ordsets:ordset(node()),
                %% Failure detector
                fd = ordsets:new() :: ordsets:ordset(node())
               }).

-type proto_messages() :: {halt, election_id()} |
                          {ack, election_id()} |
                          {rej, election_id()} |
                          {norm_p, election_id()} |
                          {leader, election_id()} |
                          {not_norm, election_id()}.

-type proto_states() :: norm | wait | elec2.

-define(ALPHA_TIME, timer:seconds(10)).

%%====================================================================
%% API
%%====================================================================
start_link(Net) when is_list(Net) ->
    gen_fsm:start_link({local, ?MODULE}, ?MODULE, [Net], []).

%%====================================================================
%% gen_fsm callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, StateName, State} |
%%                         {ok, StateName, State, Timeout} |
%%                         ignore                              |
%%                         {stop, StopReason}
%% Description:Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/3,4, this function is called by the new process to
%% initialize.
%%--------------------------------------------------------------------
init([Net]) when is_list(Net) ->
    timer:send_interval(?ALPHA_TIME, periodically),
    net_kernel:monitor_nodes(true, [{node_type, visible}]),
    {ok, recovery, #state{peers=ordsets:from_list(Net)}, 0}.


%%--------------------------------------------------------------------
%% Function:
%% state_name(Event, State) -> {next_state, NextStateName, NextState}|
%%                             {next_state, NextStateName,
%%                                NextState, Timeout} |
%%                             {stop, Reason, NewState}
%% Description:There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_event/2, the instance of this function with the same name as
%% the current state name StateName is called to handle the event. It is also
%% called if a timeout occurs.
%%--------------------------------------------------------------------

recovery(timeout, State) ->
    start_stage2(State#state{incarn = erlang:now()}).

norm(_Evt, State) -> {next_state, norm, State}.
elec2(_Evt, State) -> {next_state, elec2, State}.
wait(_Evt, State) -> {next_state, wait, State}.

halting(T, J, State = #state{peers = Peers}) ->
    [play_dead(N) || N <- ordsets:to_list(Peers),
                     node() < N ],
    send(J, {ack, T}),
    {next_state, wait, start_fd(J, State#state{elid=T})}.

start_stage2(State = #state{incarn = Incarn,
                            nextel = Nextel,
                            peers = Peers}) ->
    [play_alive(N) || N <- ordsets:to_list(Peers),
                      N > node() ],
    NewState = State#state{elid = {node(), Incarn, Nextel},
                           nextel = Nextel + 1,
                           acks = ordsets:new(),
                           pendack = node()},
    contin_stage2(NewState).

contin_stage2(State = #state{pendack = P,
                             peers = Peers,
                             elid = Elid}) ->
    case max_p(Peers) of
        N when P < N ->
            Pendack = next_p(P, Peers),
            send(Pendack, {halt, Elid}),
            {next_state, elec2, start_fd(Pendack,State#state{pendack=Pendack})};
        _ ->
            %% I'm the leader.
            ?INFO("I ~p became the leader.", [node()]),
            [send(Node, {leader, Elid})
             || Node <- ordsets:to_list(State#state.acks)],
            NewState = State#state{leader=node()},
            {next_state, norm, NewState}
    end.

periodically(norm, #state{leader=Self,
                          peers=Peers,
                          elid=Elid}) when Self =:= node() ->
    [ send(P, {norm_p, Elid})
      || P <- ordsets:to_list(Peers),
         P > node() ];
periodically(_StateName, _State) ->
    ok.

handle_event({?MODULE, J, {halt, T}}, StateName,
             State = #state{leader=Ldr,
                            elid=Elid})
  when StateName =:= norm, Ldr < J;
       StateName =:= wait, element(1, Elid) < J ->
    send(J, {rej, T}),
    {next_state, StateName, State};
handle_event({?MODULE, J, {halt, T}}, _StateName,
             State = #state{}) ->
    halting(T, J, State);

handle_event({?MODULE, J, downsig}, StateName,
             State = #state{elid=Elid,
                            leader=Ldr})
  when StateName =:= norm, J =:= Ldr;
       StateName =:= wait, J =:= element(1, Elid) ->
    start_stage2(State);

handle_event({?MODULE, J, downsig}, elec2,
             State = #state{pendack=J}) ->
    contin_stage2(State);

handle_event({?MODULE, J, {rej, _T}}, elec2,
             State = #state{pendack=J}) ->
    contin_stage2(State);

handle_event({?MODULE, J, {ack, T}}, elec2,
             State = #state{elid=T,
                            pendack=J,
                            acks=Acks}) ->
    contin_stage2(State#state{acks = ordsets:add_element(J, Acks)});

handle_event({?MODULE, J, {leader, T}}, wait,
             State = #state{elid=T}) ->
    {next_state, norm, set_fds([J], State#state{leader=J})};

handle_event({?MODULE, J, {norm_p, T}}, StateName,
             State = #state{elid=Elid,
                            leader = Leader})
  when StateName =/= norm, J < element(1, Elid);
       StateName =:= norm, J < Leader ->
    send(J, {not_norm, T}),
    {next_state, StateName, State};

handle_event({?MODULE, J, {norm_p, T}}, StateName,
             State = #state{elid = T,
                            leader = J}) ->
    %% Everything is fine.
    {next_state, StateName, State};

handle_event({?MODULE, _J, {not_norm, T}}, norm,
             State = #state{elid = T,
                            leader = Self})
  when Self =:= node() ->
    start_stage2(State);

handle_event(Msg, StateName, State) ->
    ?INFO("~p: ignored ~p", [StateName, Msg]),
    {next_state, StateName, State}.


%%--------------------------------------------------------------------
%% Function:
%% state_name(Event, From, State) -> {next_state, NextStateName, NextState} |
%%                                   {next_state, NextStateName,
%%                                     NextState, Timeout} |
%%                                   {reply, Reply, NextStateName, NextState}|
%%                                   {reply, Reply, NextStateName,
%%                                    NextState, Timeout} |
%%                                   {stop, Reason, NewState}|
%%                                   {stop, Reason, Reply, NewState}
%% Description: There should be one instance of this function for each
%% possible state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_event/2,3, the instance of this function with the same
%% name as the current state name StateName is called to handle the event.
%%--------------------------------------------------------------------
%% state_name(_Event, _From, State) ->
%%     {next_state, Reply, state_name, State}.

%%--------------------------------------------------------------------
%% Function:
%% handle_event(Event, StateName, State) -> {next_state, NextStateName,
%%						  NextState} |
%%                                          {next_state, NextStateName,
%%					          NextState, Timeout} |
%%                                          {stop, Reason, NewState}
%% Description: Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%--------------------------------------------------------------------
%handle_event(_Event, StateName, State) ->
%    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% Function:
%% handle_sync_event(Event, From, StateName,
%%                   State) -> {next_state, NextStateName, NextState} |
%%                             {next_state, NextStateName, NextState,
%%                              Timeout} |
%%                             {reply, Reply, NextStateName, NextState}|
%%                             {reply, Reply, NextStateName, NextState,
%%                              Timeout} |
%%                             {stop, Reason, NewState} |
%%                             {stop, Reason, Reply, NewState}
%% Description: Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/2,3, this function is called to handle
%% the event.
%%--------------------------------------------------------------------

handle_sync_event(_Event, _From, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% Function:
%% handle_info(Info,StateName,State)-> {next_state, NextStateName, NextState}|
%%                                     {next_state, NextStateName, NextState,
%%                                       Timeout} |
%%                                     {stop, Reason, NewState}
%% Description: This function is called by a gen_fsm when it receives any
%% other message than a synchronous or asynchronous event
%% (or a system message).
%%--------------------------------------------------------------------

handle_info({nodedown, J, _Info}, StateName, State = #state{}) ->
    %% Check if this nodedown is relevant to the
    %% leader election algorithm
    case filter_nodedown(J, State) of
        nodedown ->
            handle_event({?MODULE, J, downsig},
                         StateName, State);
        ignore ->
            {next_state, StateName, State}
    end;

handle_info(periodically, StateName, State) ->
    periodically(StateName, State),
    {next_state, StateName, State};

handle_info(Info, StateName, State) ->
    ?INFO("~p ignored unexpected message: ~p",
          [StateName, Info]),
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, StateName, State) -> void()
%% Description:This function is called by a gen_fsm when it is about
%% to terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, #state{}) ->
    ok.

%%--------------------------------------------------------------------
%% Function:
%% code_change(OldVsn, StateName, State, Extra) -> {ok, StateName, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

send(Node, Msg) ->
    gen_fsm:send_all_state_event(server_on(Node), {?MODULE, node(), Msg}).

play_dead(Node) ->
    server_on(Node) ! {nodedown, node(), [{?MODULE, {playing_dead, node()}}]}.

play_alive(Node) ->
    server_on(Node) ! {nodeup, node(), [{?MODULE, {playing_dead, node()}}]}.

server_on(Node) ->
    {?MODULE, Node}.

max_p(Nodes) -> lists:max(ordsets:to_list(Nodes)).

next_p(N, Nodes) ->
    case lists:dropwhile(fun (Node) -> Node =< N end,
                         ordsets:to_list(Nodes)) of
        [] -> N; %%% Is this the right thing to do?
        [Node | _ ] -> Node
    end.

start_fd(Node, State = #state{fd = FDs}) ->
    State#state{fd=ordsets:add_element(Node, FDs)}.

set_fds(Nodes, State = #state{fd = FDs}) ->
    NewFDs = ordsets:union(FDs, ordsets:from_list(Nodes)),
    State#state{fd = NewFDs}.

filter_nodedown(Node, #state{fd = FDs}) ->
    case ordsets:is_element(Node, FDs) of
        true ->
            nodedown;
        false ->
            ignore
    end.
