CXX      := g++
CXXFLAGS := -std=c++17 -O3 -march=native -Wall -Wextra -pthread
LDFLAGS  := -pthread
INC      := include
BUILD    := build
BIN      := bin

SERVER_OBJS := $(BUILD)/server_main.o $(BUILD)/server.o $(BUILD)/executor.o \
               $(BUILD)/parser.o $(BUILD)/storage.o $(BUILD)/lru_cache.o
CLIENT_OBJS := $(BUILD)/client_main.o $(BUILD)/flexql.o
BENCH2_OBJS := $(BUILD)/benchmark_flexql.o $(BUILD)/flexql.o

.PHONY: all clean bench2 reset-data
all: $(BIN)/flexql-server $(BIN)/flexql-client $(BIN)/benchmark_flexql

bench2: $(BIN)/benchmark_flexql

reset-data:
	rm -rf flexql_data

$(BUILD)/%.o: src/%.cpp
	@mkdir -p $(BUILD) $(BIN)
	$(CXX) $(CXXFLAGS) -I$(INC) -c $< -o $@

$(BUILD)/benchmark_flexql.o: benchmark_flexql.cpp
	@mkdir -p $(BUILD) $(BIN)
	$(CXX) $(CXXFLAGS) -I$(INC) -c $< -o $@

$(BIN)/flexql-server: $(SERVER_OBJS)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

$(BIN)/flexql-client: $(CLIENT_OBJS)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

$(BIN)/benchmark_flexql: $(BENCH2_OBJS)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

clean:
	rm -rf $(BUILD) $(BIN)
