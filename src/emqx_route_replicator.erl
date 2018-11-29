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

-module(emqx_route_replicator).

-behaviour(gen_statem).

%% API
-export([start_link/0, commit/2]).

%% gen_statem callbacks
-export([running/3]).
-export([init/1, callback_mode/0, terminate/3, code_change/4]).

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-record(route_repl, {
          router :: pid(),
          updates :: list()
         }).

-define(ROUTE, emqx_route).
-define(ROUTE_REPL, emqx_route_repl).

%%-----------------------------------------------------------------------------
%% Mnesia bootstrap
%%-----------------------------------------------------------------------------

mnesia(boot) ->
    ok = ekka_mnesia:create_table(?ROUTE_REPL, [
                {type, set},
                {ram_copies, [node()]},
                {record_name, route_repl},
                {attributes, record_info(fields, route_repl)}]);
mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?ROUTE_REPL).

start_link() ->
    gen_statem:start_link({local, ?MODULE}, ?MODULE, [], []).

commit(Router, Updates) ->
    mnesia:write(?ROUTE_REPL, #route_repl{router = Router, updates = Updates}, write).

%%-----------------------------------------------------------------------------
%% gen_statem callbacks
%%-----------------------------------------------------------------------------

init([]) ->
    mnesia:subscribe({table, ?ROUTE_REPL, simple}),
    {ok, running, #{}}.

callback_mode() -> state_functions.

running(info, {mnesia_table_event, {write, {?ROUTE_REPL, Router, Updates}, _}}, State)
    when node(Router) == node() ->
    {keep_state, State};

running(info, {mnesia_table_event, {write, {?ROUTE_REPL, Router, Updates}, _}}, State) ->
    io:format("[Route Repl] Remote repl from ~s: router = ~p, count = ~w~n",
              [node(Router), Router, length(Updates)]),
    mnesia:ets(fun lists:foreach/2, [fun update_route/1, Updates]),
    {keep_state, State};

running(EventType, EventContent, State) ->
    emqx_logger:error("[RouteRepl] unexpected event(~p, ~p)", [EventType, EventContent]),
    {keep_state, State}.

terminate(_Reason, _StateName, _State) ->
    mnesia:unsubscribe({table, ?ROUTE_REPL, simple}),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

update_route({add, Route}) ->
    mnesia:write(?ROUTE, Route, write);
update_route({del, Route}) ->
    mnesia:delete_object(?ROUTE, Route, write);
update_route(Other) ->
    emqx_logger:error("[RouteRepl] unexpected changelog: ~p", [Other]).

