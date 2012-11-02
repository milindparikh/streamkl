-module(streamklw3_socket_server).
-behaviour(gen_server).

-define(REFRESHRATE, 5000).   % default of 5 seconds
-define(MAXNODATA, 10).	      % this determines how long will the loop 
		   	      % continue with no data
%% API function 
-export([start_link/3]).


%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).


-record(state, {
	       sock, 
	       port, 
	       peer_addr, 
	       peer_port, 
	       refresh_rate,
	       streamers,
               no_data_counters,
 	       http_state        % a=accept, s=stream
		}).

-record(request, {
		 topic, 
 		 style=default, 
		 broker=0,
		 partition=0, 
		 offset=0
		 }
        ).


start_link(Listen_pid, Listen_socket, ListenPort) -> 
   gen_server:start_link(?MODULE, {Listen_pid, Listen_socket, ListenPort}, []).


init({Listen_pid, Listen_socket, ListenPort}) ->
    case gen_tcp:accept(Listen_socket) of
	{ok, Socket} ->
	    streamklw3_server:create(Listen_pid, self()),
	    {ok, {Addr, Port}} = inet:peername(Socket),
            State = #state{sock = Socket,
                   port = ListenPort,
                   peer_addr = Addr,
                   peer_port = Port,
		   http_state=a,
		   refresh_rate = ?REFRESHRATE},
            {ok, State, 0};
	Else ->
	    error_logger:error_report([{application, iserve},
				       "Accept failed error",
				       io_lib:format("~p",[Else])]),
            {stop, Else}

    end.


handle_info(timeout, #state{http_state=a, sock=Sock, refresh_rate=RefreshRate} = State) ->       

	 
	 NewState_1 = attach_streamers(State), 

         gen_tcp:send(Sock, "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=UTF-8\r\nCache-Control: no-cache\r\nTransfer-Encoding: chunked\r\n\r\n"),    % so we are streaming

	 {noreply, NewState_1, RefreshRate};


handle_info(timeout, #state{streamers=Streamers, http_state=s, sock=Sock, refresh_rate=RefreshRate} = State) ->       

      case Streamers of 
      	   []-> send_chunk(Sock, ""),
	        {stop, streamers_done, State};
	   _ -> 

	         NewState = call_streamers(State),
      		 case is_connection_closed(Sock) of
      	   	      true ->
               	      	   {stop, connection_died, State};
	              false -> 
		           { noreply, 
	         	     NewState,
                 	     RefreshRate
                           }   
                 end
       end;

       

handle_info(_Info, State) ->
    {noreply, State}.


handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.


handle_cast(_Msg, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.




attach_streamers(State) -> 

     {Streamers, ListCountersNoData} = 
     		     lists:foldl(fun ({Topic, Broker, Partition, Offset}, {Acc1, Acc2}) -> 
       		       		        {[erlkafka:get_kafka_stream_consumer( Broker, Topic, Partition, Offset) | Acc1], [0|Acc2]}
		   		 end, 
		                 {[], []}, 
                                 parse_header(State)),


     State#state{streamers=Streamers ,
                 no_data_counters = ListCountersNoData,
		 http_state=s}.



call_streamers(#state{streamers=Streamers, sock=Sock} = State) -> 
   {NewState, _} = 


      lists:foldl(fun ({Stream,_, Pid}, {State_1, Count}) ->
                 case Stream(Pid) of 
      	           {ok, no_data} -> 
                      {IncrModifiedStreamers, IncrModifiedCounters} = 
                           incr_counters(State_1#state.streamers, State_1#state.no_data_counters, Count),
		      {State_1#state{streamers=IncrModifiedStreamers, 
		      	           no_data_counters=IncrModifiedCounters}, Count+1};

                   {ok, {Data, _Size}} -> 
		      lists:foldl(fun (H, _Acc1) -> 
			             case send_chunk(Sock,H) of 
			               false -> throw(false);
				       true -> true
				     end
                                   end, 
                                   true, 
                                   Data), 
                       IncrModifiedCounters = 
                             set_counters( State_1#state.no_data_counters, Count,0),
		       {State_1#state{no_data_counters = IncrModifiedCounters}, Count+1}

                   end
		end, 
		{State, 1},
                Streamers), 
        NewState.



delete_poss_from_list(Positions, OrigList) -> 
         {_, L} = 
    		   lists:foldl(fun(Element, {Count, Acc1}) -> 
				    try 
				        lists:foldl(fun(Position, _Acc2) -> 
				      	                case Position =:= Count of 
						     	  true -> throw(true);
							  false -> false
				      	                 end
					             end,
						    false,
						    Positions),
				         throw(false)
				    catch
                                         throw:true -> {Count+1, Acc1};
					 throw:false -> {Count+1, [Element|Acc1]}
                                    end
                                 end, 
			       {1, []},
			       OrigList),
            lists:reverse(L).


incr_and_trunc_counters (Counters, Which) -> 
	 {_, {TL, TC}} = 
            lists:foldl (fun (Counter, {Count, {TruncatedList, TruncatedCounts}}) -> 
	    		   case Count =:= Which of 
			   	true -> 
                           	     case Counter+1 >= ?MAXNODATA of 
			   	     	  true  ->  {Count+1, {TruncatedList, [Count | TruncatedCounts]}};
                                  	  false ->  {Count+1, {[Counter+1|TruncatedList] ,TruncatedCounts}}
			              end;
				false -> 
				     {Count+1, {[Counter|TruncatedList] ,TruncatedCounts}}
				      
                            end
                      end,
		      {1, {[], []}},
		      Counters),
         {lists:reverse(TL), lists:reverse(TC)}.


incr_counters (Streamers, Counters, Count) -> 
     {TruncatedCounters, RemovedCounterPos} = 
     			 incr_and_trunc_counters(Counters, Count),

     lists:foreach(fun (Elem) -> 
     		       	      {_, Terminate, Pid} = lists:nth(Elem, Streamers),
                              Terminate(Pid)
	           end,
                   RemovedCounterPos),

			      
     TruncatedStreamers  = delete_poss_from_list(RemovedCounterPos, Streamers),
     {TruncatedStreamers,  TruncatedCounters}.


set_counters(Counters, Count, Value) -> 
    {_, L} = 
    lists:foldl(fun (X, {VarCount, Acc}) -> 
                     case VarCount =:= Count of 
		     	  true  -> {VarCount + 1, [Value|Acc]};
			  false -> {VarCount + 1, [X|Acc]}
		     end
                end, 
		{1, []},
		Counters),
    lists:reverse(L).

		            			                          



send_chunk(Sock, Data) -> 

  case Data of 
    []-> gen_tcp:send(Sock, 0);
    _ ->

      Length=io_lib:format("~.16B", [length(binary_to_list(Data))]),
      try 
	case gen_tcp:send(Sock, Length) of 
              {error, Reason} -> throw ({error, Reason});
              ok -> 
	           case gen_tcp:send(Sock, "\r\n") of 
    	                {error, Reason} -> throw ({error, Reason});
			ok -> 
		  	      case gen_tcp:send(Sock, Data) of 
    	                           {error, Reason} -> throw ({error, Reason});
				   ok-> 
				       case gen_tcp:send(Sock, "\r\n") of 
				            {error, Reason} -> throw ({error, Reason});
					    ok-> 
					       throw(ok)
					end
			       end
                    end
         end 
      catch
           throw:{error, _} -> false;
	   throw:ok	    -> case is_connection_closed(Sock) of true->false;false->true end
      end
    end.


is_connection_closed(Socket) -> 
   inet:setopts(Socket, [{active, once}]),
   
   receive
     {tcp_closed, Socket} ->
         true;
     _Data ->
        false
   after 
       1000-> false 
   end.

% http://hostname:port/stream/test-topic/default-style/0/0/0
% http://hostname:port/stream/test-topic/default-style/bpo=0:0:0&bpo=0:1:0



    
parse_header(#state{http_state=a, sock=Sock}) -> 
    inet:setopts(Sock, [{active, once}]),

    receive 
       {http, Sock, Msg } -> 
            {http_request, 'GET', {abs_path, Path}, {1,1}} = Msg,
	    parse_path(Path)
    end.


   
parse_path(Path) -> 
         {_NewCount, NewRequest} 
             = 
	        lists:foldl(fun (X, {Count, Request}) -> 
                               case Count of 
		     	          1 -> 
			      	     Request_1 = Request#request{topic = list_to_binary(X)},
			      	     {Count+1, Request_1    };
		     	  	  2 -> 
				     Request_1 = Request#request{style = X},
			      	     {Count+1,     Request_1};
		     	  	  3 -> 
			      	     Request_1 = Request#request{broker = list_to_integer(X)},
			      	     {Count+1,     Request_1};
		     	  	  4 -> 
			      	     Request_1 = Request#request{partition = list_to_integer(X)},
			      	     {Count+1,    Request_1};
		     	  	  5 -> 
			      	     Request_1 = Request#request{offset = list_to_integer(X)},
			      	     {Count+1,     Request_1};
			  	  _Default ->
			    	     Request_1 = Request#request{},
                            	     {Count+1, Request_1}
                     		end
			     end,
                          {0, #request{}},
	   	          re:split(Path, "/", [{return, list}])
			  ),
        [{NewRequest#request.topic,      
          NewRequest#request.broker,      
          NewRequest#request.partition,      
          NewRequest#request.offset}].
      