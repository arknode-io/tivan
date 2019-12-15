.PHONY: default all build clean compile release shell

export ERLANG_ROCKSDB_OPTS=-DWITH_SYSTEM_ROCKSDB=ON -DWITH_SNAPPY=ON -DWITH_LZ4=ON

rebar='rebar3'

default: compile
all: clean compile test
compile:
	@$(rebar) compile
clean:
	@$(rebar) clean
cleanall:
	@$(rebar) clean -a
test:
	@$(rebar) do ct
shell:
	@$(rebar) shell
