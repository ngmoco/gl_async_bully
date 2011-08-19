
-type event_return(State) :: {'ok', State} |
                        {'ok', Sync::term(), State} |
                        {stop, Reason::term(), State}.

-type call_return(State) :: {'reply', Reply::term(), State} |
                            {'reply', Reply::term(), Broadcast::term(), State} |
                            {'noreply', State} |
                            {'stop', Reason::term(), Reply::term(), State}.

-spec init (Args::term()) ->
                   {'ok', State::term()} |
                   'ignore' |
                   {'stop', Reason::term()}.

%% elected(_,_,node()) - captured a single candidate
%%   reply notifies only the new candidate of the
%%   sync state, ok notifies all candidates
%% elected(_,_,undefined) - won a general election
%%   ok broadcasts sync to all candidates
-spec elected(gl_async_bully:cluster_info(),
              State
             ) -> event_return(State).

-spec handle_leader_call(Request::term(),
                         From::{pid(), reference()},
                         gl_async_bully:cluster_info(),
                         State
                        ) -> call_return(State).

-spec handle_leader_cast(Msg::term(),
                         gl_async_bully:cluster_info(),
                         State
                        ) -> event_return(State).

-spec from_leader(Msg::term(),
                  gl_async_bully:cluster_info(),
                  State
                 ) -> event_return(State).

-spec handle_call(Request::term(),
                  From::{pid(), reference()},
                  gl_async_bully:cluster_info(),
                  State
                  ) -> call_return(State).

-spec handle_cast(Msg::term(),
                  gl_async_bully:cluster_info(),
                  State
                 ) -> event_return(State).

-spec handle_info(Msg::term(),
                  gl_async_bully:cluster_info(),
                  State
                 ) -> event_return(State).

-spec terminate(Reason::term(),
                gl_async_bully:cluster_info(),
                State::term()
               ) -> any().

-spec code_change(OldVsn::term(),
                  State,
                  Extra::term()) -> {'ok', State}.

-spec format_status('normal' | 'terminate',
                    [proplists:proplist(), State::term()]) -> list().
