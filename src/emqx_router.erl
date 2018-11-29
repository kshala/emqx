%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(emqx_router).

-behaviour(gen_server).

-include("emqx.hrl").
-include_lib("ekka/include/ekka.hrl").

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-export([start_link/2]).

%% Route APIs
-export([add_route/1, add_route/2, add_route/3]).
-export([get_routes/1]).
-export([del_route/1, del_route/2, del_route/3]).
-export([has_routes/1, match_routes/1, print_routes/1]).
-export([topics/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-type(destination() :: node() | {binary(), node()}).

-record(state, {pool, id, batch :: undefined | emqx_batch:batch()}).

-define(ROUTE, emqx_route).

%%-----------------------------------------------------------------------------
%% Mnesia bootstrap
%%-----------------------------------------------------------------------------

mnesia(boot) ->
    ok = ekka_mnesia:create_table(?ROUTE, [
                {type, bag},
                {ram_copies, [node()]},
                {record_name, route},
                {attributes, record_info(fields, route)},
                {storage_properties, [{ets, [{read_concurrency, true},
                                             {write_concurrency, true}]}]}]);
mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?ROUTE).

%%------------------------------------------------------------------------------
%% Strat a router
%%------------------------------------------------------------------------------

-spec(start_link(atom(), pos_integer()) -> {ok, pid()} | ignore | {error, term()}).
start_link(Pool, Id) ->
    gen_server:start_link({local, emqx_misc:proc_name(?MODULE, Id)},
                          ?MODULE, [Pool, Id], [{hibernate_after, 2000}]).

%%------------------------------------------------------------------------------
%% Route APIs
%%------------------------------------------------------------------------------

-spec(add_route(emqx_topic:topic() | emqx_types:route()) -> ok).
add_route(Topic) when is_binary(Topic) ->
    add_route(#route{topic = Topic, dest = node()});
add_route(Route = #route{topic = Topic}) ->
    cast(pick(Topic), {add_route, Route}).

-spec(add_route(emqx_topic:topic(), destination()) -> ok).
add_route(Topic, Dest) when is_binary(Topic) ->
    add_route(#route{topic = Topic, dest = Dest}).

-spec(add_route({pid(), reference()}, emqx_topic:topic(), destination()) -> ok).
add_route(From, Topic, Dest) when is_binary(Topic) ->
    cast(pick(Topic), {add_route, From, #route{topic = Topic, dest = Dest}}).

-spec(get_routes(emqx_topic:topic()) -> [emqx_types:route()]).
get_routes(Topic) ->
    ets:lookup(?ROUTE, Topic).

-spec(del_route(emqx_topic:topic() | emqx_types:route()) -> ok).
del_route(Topic) when is_binary(Topic) ->
    del_route(#route{topic = Topic, dest = node()});
del_route(Route = #route{topic = Topic}) ->
    cast(pick(Topic), {del_route, Route}).

-spec(del_route(emqx_topic:topic(), destination()) -> ok).
del_route(Topic, Dest) when is_binary(Topic) ->
    del_route(#route{topic = Topic, dest = Dest}).

-spec(del_route({pid(), reference()}, emqx_topic:topic(), destination()) -> ok).
del_route(From, Topic, Dest) when is_binary(Topic) ->
    cast(pick(Topic), {del_route, From, #route{topic = Topic, dest = Dest}}).

-spec(has_routes(emqx_topic:topic()) -> boolean()).
has_routes(Topic) when is_binary(Topic) ->
    ets:member(?ROUTE, Topic).

-spec(topics() -> list(emqx_topic:topic())).
topics() -> mnesia:dirty_all_keys(?ROUTE).

%% @doc Match routes
%% Optimize: routing table will be replicated to all router nodes.
-spec(match_routes(emqx_topic:topic()) -> [emqx_types:route()]).
match_routes(Topic) when is_binary(Topic) ->
    Matched = mnesia:ets(fun emqx_trie:match/1, [Topic]),
    lists:append([get_routes(To) || To <- [Topic | Matched]]).

%% @doc Print routes to a topic
-spec(print_routes(emqx_topic:topic()) -> ok).
print_routes(Topic) ->
    lists:foreach(fun(#route{topic = To, dest = Dest}) ->
                      io:format("~s -> ~s~n", [To, Dest])
                  end, match_routes(Topic)).

cast(Router, Msg) ->
    gen_server:cast(Router, Msg).

pick(Topic) ->
    gproc_pool:pick_worker(router, Topic).

%%-----------------------------------------------------------------------------
%% gen_server callbacks
%%-----------------------------------------------------------------------------

init([Pool, Id]) ->
    rand:seed(exsplus, erlang:timestamp()),
    gproc_pool:connect_worker(Pool, {Pool, Id}),
    BatchRepl = init_batch_repl(emqx_config:get_env(route_batch_replication, false)),
    io:format("BatchRepl: ~p~n", [BatchRepl]),
    {ok, #state{pool = Pool, id = Id, batch = BatchRepl}}.

init_batch_repl(false) ->
    undefined;
init_batch_repl(true) ->
    emqx_batch:init(
      #{batch_size => emqx_config:get_env(route_batch_size, 1000),
        linger_ms => emqx_config:get_env(route_linger_ms, 5),
        commit_fun => fun batch_replicate/1}).

handle_call(Req, _From, State) ->
    emqx_logger:error("[Router] unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({add_route, From, Route}, State) ->
    {noreply, NewState} = handle_cast({add_route, Route}, State),
    _ = gen_server:reply(From, ok),
    {noreply, NewState};

handle_cast({add_route, Route = #route{topic = Topic, dest = Dest}}, State) ->
    noreply(
      case lists:member(Route, get_routes(Topic)) of
          true  -> State;
          false ->
              ok = emqx_router_helper:monitor(Dest),
              case emqx_topic:wildcard(Topic) of
                  true ->
                      log(trans(fun add_trie_route/1, [Route])),
                      State;
                  false ->
                      add_direct_route(Route, State)
              end
      end);

handle_cast({del_route, From, Route}, State) ->
    {noreply, NewState} = handle_cast({del_route, Route}, State),
    _ = gen_server:reply(From, ok),
    {noreply, NewState};

handle_cast({del_route, Route = #route{topic = Topic, dest = Dest}}, State) when is_tuple(Dest) ->
    noreply(
      case emqx_topic:wildcard(Topic) of
          true  -> log(trans(fun del_trie_route/1, [Route])),
                   State;
          false -> del_direct_route(Route, State)
      end);

handle_cast({del_route, Route = #route{topic = Topic}}, State) ->
    %% Confirm if there are still subscribers...
    {noreply, case ets:member(emqx_subscriber, Topic) of
                  true  -> State;
                  false ->
                      case emqx_topic:wildcard(Topic) of
                          true  -> log(trans(fun del_trie_route/1, [Route])),
                                   State;
                          false -> del_direct_route(Route, State)
                      end
              end};

handle_cast(Msg, State) ->
    emqx_logger:error("[Router] unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(batch_linger_expired, State = #state{batch = Batch}) ->
    {noreply, State#state{batch = emqx_batch:commit(Batch)}, hibernate};

handle_info(Info, State) ->
    emqx_logger:error("[Router] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{pool = Pool, id = Id}) ->
    %%_ = cacel_batch_timer(Batch),
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%-----------------------------------------------------------------------------
%% Internal functions
%%-----------------------------------------------------------------------------

add_direct_route(Route, State = #state{batch = undefined}) ->
    mnesia:async_dirty(fun mnesia:write/3, [?ROUTE, Route, sticky_write]),
    State;
add_direct_route(Route, State = #state{batch = Batch}) ->
    ets:insert(?ROUTE, Route),
    %%mnesia:ets(fun mnesia:write/3, [?ROUTE, Route, write]),
    State#state{batch = emqx_batch:push({add, Route}, Batch)}.

del_direct_route(Route, State = #state{batch = undefined}) ->
    mnesia:async_dirty(fun mnesia:delete_object/3, [?ROUTE, Route, sticky_write]),
    State;
del_direct_route(Route, State = #state{batch = Batch}) ->
    mnesia:ets(fun mnesia:delete_object/3, [?ROUTE, Route, write]),
    State#state{batch = emqx_batch:push({del, Route}, Batch)}.

batch_replicate(Updates) ->
    mnesia:sync_dirty(fun emqx_route_replicator:commit/2, [self(), Updates]).

add_trie_route(Route = #route{topic = Topic}) ->
    case mnesia:wread({?ROUTE, Topic}) of
        [] -> emqx_trie:insert(Topic);
        _  -> ok
    end,
    mnesia:write(?ROUTE, Route, sticky_write).

del_trie_route(Route = #route{topic = Topic}) ->
    case mnesia:wread({?ROUTE, Topic}) of
        [Route] -> %% Remove route and trie
                   mnesia:delete_object(?ROUTE, Route, sticky_write),
                   emqx_trie:delete(Topic);
        [_|_]   -> %% Remove route only
                   mnesia:delete_object(?ROUTE, Route, sticky_write);
        []      -> ok
    end.

%% @private
-spec(trans(function(), list(any())) -> ok | {error, term()}).
trans(Fun, Args) ->
    case mnesia:transaction(Fun, Args) of
        {atomic, _}      -> ok;
        {aborted, Error} -> {error, Error}
    end.

log(ok) -> ok;
log({error, Reason}) ->
    emqx_logger:error("[Router] mnesia aborted: ~p", [Reason]).

noreply(State) -> {noreply, State}.
