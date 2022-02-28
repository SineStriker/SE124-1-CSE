#ifndef raft_h
#define raft_h

#include <algorithm>
#include <atomic>
#include <chrono>
#include <ctime>
#include <mutex>
#include <stdarg.h>
#include <thread>

#include "raft_protocol.h"
#include "raft_state_machine.h"
#include "raft_storage.h"
#include "rpc.h"

inline int64_t Time() {
    // CLOCK_REALTIME:系统相对时间,从UTC 1970-1-1 0:0:0开始计时,更改系统时间会更改获取的值;
    // CLOCK_MONOTONIC:系统绝对时间/单调时间,为系统重启到现在的时间,更改系统时间对它没有影响;
    // CLOCK_PROCESS_CPUTIME_ID:本进程到当前代码系统CPU花费的时间;
    // CLOCK_THREAD_CPUTIME_ID:本线程到当前代码系统CPU花费的时间;
    struct timespec ts {};
    clock_gettime(CLOCK_REALTIME, &ts);
    int64_t milliseconds = (ts.tv_sec * 1000) + (double(ts.tv_nsec) / 1000000);
    return milliseconds;
}

template <typename state_machine, typename command> class raft {

    static_assert(std::is_base_of<raft_state_machine, state_machine>(),
                  "state_machine must inherit from raft_state_machine");
    static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");

    friend class thread_pool;

#define RAFT_LOG(fmt, args...)                                                                                         \
    do {                                                                                                               \
        auto now =                                                                                                     \
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()) \
                .count();                                                                                              \
        printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args);       \
    } while (0);

public:
    raft(rpcs *rpc_server, std::vector<rpcc *> rpc_clients, int idx, raft_storage<command> *storage,
         state_machine *state);
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node.
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped().
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false.
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx; // A big lock to protect the whole data structure
    ThrPool *thread_pool;
    raft_storage<command> *storage; // To persist the raft log
    state_machine *state;           // The state machine that applies the raft log, e.g. a kv store

    rpcs *rpc_server;                // RPC server to recieve and handle the RPC requests
    std::vector<rpcc *> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                       // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;

    enum raft_role { follower, candidate, leader };
    raft_role role;
    int current_term;

    int commitIndex, lastApplied;                       // Added
    std::vector<int> nextIndex, matchIndex, matchCount; // Added
    std::vector<char> snapshot;                         // Added
    std::vector<log_entry<command>> logs;               // Added
    int votedFor;                                       // Added
    int64_t lastTime, timeout;                          // Added
    int voteCount;                                      // Added

    std::thread *background_election;
    std::thread *background_ping;
    std::thread *background_commit;
    std::thread *background_apply;

    // Your code here:

private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply &reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply &reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply &reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command> &arg,
                                     const append_entries_reply &reply);

    void send_install_snapshot(int target, install_snapshot_args arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args &arg,
                                       const install_snapshot_reply &reply);

private:
    bool is_stopped();
    int num_nodes() { return rpc_clients.size(); }

    // background workers
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:
    void setRole(raft_role r);
    std::vector<log_entry<command>> getEntries(int begin_index, int end_index) const;
};

template <typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs *server, std::vector<rpcc *> clients, int idx, raft_storage<command> *storage,
                                   state_machine *state)
    : storage(storage), state(state), rpc_server(server), rpc_clients(clients), my_id(idx), stopped(false),
      role(follower), current_term(0), background_election(nullptr), background_ping(nullptr),
      background_commit(nullptr), background_apply(nullptr) {
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here:
    // Do the initialization

    if (!snapshot.empty()) {
        state->apply_snapshot(snapshot);
    }

    logs.assign(1, log_entry<command>());

    commitIndex = logs.front().index;
    lastApplied = logs.front().index;

    voteCount = 0;
    votedFor = -1;

    // leader volatile states
    nextIndex.assign(num_nodes(), 1);
    matchIndex.assign(num_nodes(), 0);

    lastTime = Time();
    timeout = 400;
}

template <typename state_machine, typename command> raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template <typename state_machine, typename command> void raft<state_machine, command>::stop() {
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template <typename state_machine, typename command> bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template <typename state_machine, typename command> bool raft<state_machine, command>::is_leader(int &term) {
    term = current_term;
    return role == leader;
}

template <typename state_machine, typename command> void raft<state_machine, command>::start() {
    // Your code here:

    RAFT_LOG("start");
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);

    if (role != leader) {
        return false;
    }

    index = logs.back().index + 1;
    term = current_term;

    log_entry<command> log;

    log.index = index;
    log.term = term;
    log.cmd = cmd;

    logs.push_back(log);

    nextIndex[my_id] = index + 1;
    matchIndex[my_id] = index;
    matchCount.push_back(1);

    // if (!storage->append_log(entry, log.size())) {
    //     storage->update_log(log);
    // }

    return true;
}

template <typename state_machine, typename command> bool raft<state_machine, command>::save_snapshot() {
    // Your code here:

    std::unique_lock<std::mutex> lock(mtx);
    snapshot = state->snapshot();

    if (lastApplied <= logs.back().index) {
        logs.erase(logs.begin(), logs.begin() + lastApplied - logs.front().index);
    } else {
        logs.clear();
    }

    // storage->update_snapshot(snapshot);
    // storage->update_log(log);

    return true;
}

/******************************************************************

                         RPC Related

*******************************************************************/
template <typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply &reply) {
    // Your code here:

    std::unique_lock<std::mutex> lock(mtx);
    lastTime = Time();

    int ret = OK;

    reply.term = current_term;
    reply.voteGranted = false;

    if (args.term < current_term) {
        return ret;
    } else if (args.term > current_term) {
        current_term = args.term;
        setRole(follower);
    }

    log_entry<command> last_log = logs.back();
    if ((votedFor < 0 || votedFor == args.candidateId) &&
        (args.lastLogIndex >= last_log.index && args.lastLogTerm >= last_log.term)) {
        reply.voteGranted = true;
        votedFor = args.candidateId;
    }

    return ret;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args &arg,
                                                             const request_vote_reply &reply) {
    // Your code here:

    std::unique_lock<std::mutex> lock(mtx);
    if (reply.term > current_term) {
        current_term = reply.term;
        setRole(follower);
        return;
    }

    if (role != candidate) {
        return;
    }

    if (reply.voteGranted) {
        voteCount++;
    }

    if (voteCount > num_nodes() / 2) {
        setRole(leader);
    }
    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply &reply) {
    // Your code here:

    std::unique_lock<std::mutex> lock(mtx);
    lastTime = Time();

    int ret = OK;

    reply.term = current_term;
    reply.success = false;

    if (arg.term < current_term) {
        return ret;
    } else if (arg.term > current_term || role == candidate) {
        current_term = arg.term;
        setRole(follower);
    }

    log_entry<command> last_log = logs.back();
    if (arg.prevLogIndex > last_log.index || logs.at(arg.prevLogIndex - logs.front().index).term != arg.prevLogTerm) {
        return ret;
    }

    if (!arg.entries.empty()) {
        RAFT_LOG("append: %d", my_id);
        if (arg.prevLogIndex < last_log.index) {
            if (arg.prevLogIndex + 1 <= last_log.index) {
                logs.erase(logs.begin() + arg.prevLogIndex + 1 - logs.front().index, logs.end());
            }
            logs.insert(logs.end(), arg.entries.begin(), arg.entries.end());
            // storage->update_log(logs);
        } else {
            logs.insert(logs.end(), arg.entries.begin(), arg.entries.end());
            // if (!storage->append_log(arg.entries, log.size())) {
            //     storage->update_log(logs);
            // }
        }
    }

    if (arg.leaderCommit > commitIndex) {
        commitIndex = std::min(arg.leaderCommit, logs.back().index);
    }

    reply.success = true;
    return ret;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int target, const append_entries_args<command> &arg,
                                                               const append_entries_reply &reply) {
    // Your code here:

    std::unique_lock<std::mutex> lock(mtx);
    if (reply.term > current_term) {
        current_term = reply.term;
        setRole(follower);
        return;
    }

    if (role != leader) {
        return;
    }

    if (reply.success) {
        int prev = matchIndex.at(target);
        matchIndex[target] = std::max(prev, arg.prevLogIndex + int(arg.entries.size()));
        nextIndex[target] = matchIndex.at(target) + 1;

        int last = 0;
        if (prev > commitIndex) {
            last = prev - commitIndex;
        }
        last--;
        RAFT_LOG("last: %d %d %d", last, matchIndex[target], commitIndex)
        for (int i = matchIndex.at(target) - commitIndex - 1; i > last; --i) {
            ++matchCount[i];
            if (matchCount.at(i) > num_nodes() / 2 &&
                logs.at(commitIndex + i + 1 - logs.front().index).term == current_term) {
                commitIndex += i + 1;
                matchCount.erase(matchCount.begin(), matchCount.begin() + i + 1);
                break;
            }
        }
    } else {
        nextIndex[target] = std::min(arg.prevLogIndex, nextIndex.at(target));
    }
}

template <typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply &reply) {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);
    lastTime = Time();

    int ret = OK;
    reply.term = current_term;

    if (args.term < current_term) {
        return ret;
    }

    if (args.term > current_term || role == candidate) {
        current_term = args.term;
        setRole(follower);
    }

    if (args.lastIncludedIndex <= logs.back().index &&
        args.lastIncludedTerm == logs.at(args.lastIncludedIndex - logs.front().index).term) {
        int end_index = args.lastIncludedIndex;

        if (end_index <= logs.back().index) {
            logs.erase(logs.begin(), logs.begin() + end_index - logs.front().index);
        } else {
            logs.clear();
        }
    } else {
        log_entry<command> entry;
        entry.index = args.lastIncludedIndex;
        entry.term = args.lastIncludedTerm;
        logs.assign(1, entry);
    }
    snapshot = args.snapshot;
    state->apply_snapshot(snapshot);

    lastApplied = args.lastIncludedIndex;
    commitIndex = std::max(commitIndex, args.lastIncludedIndex);

    // storage->update_log(logs);
    // storage->update_snapshot(args.snapshot);

    return ret;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int target, const install_snapshot_args &arg,
                                                                 const install_snapshot_reply &reply) {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);

    if (reply.term > current_term) {
        current_term = reply.term;
        setRole(follower);
        return;
    }
    if (role != leader) {
        return;
    }

    matchIndex[target] = std::max(matchIndex.at(target), arg.lastIncludedIndex);
    nextIndex[target] = matchIndex.at(target) + 1;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template <typename state_machine, typename command> void raft<state_machine, command>::run_background_election() {
    // Check the liveness of the leader.
    // Work for followers and candidates.

    // Hints: You should record the time you received the last RPC.
    //        And in this function, you can compare the current time with it.
    //        For example:
    //        if (current_time - last_received_RPC_time > timeout) start_election();
    //        Actually, the timeout should be different between the follower (e.g. 300-500ms) and the candidate (e.g.
    //        1s).

    while (true) {
        if (is_stopped())
            return;
        // Your code here:

        if (role != leader) {
            int64_t curTime = Time();
            // RAFT_LOG("before: %d %ld %ld", my_id, curTime - lastTime, timeout)
            if (curTime - lastTime > timeout) {
                // Start election
                setRole(candidate);
                current_term++;

                request_vote_args args;
                args.term = current_term;
                args.candidateId = my_id;

                log_entry<command> last_log = logs.back();
                args.lastLogIndex = last_log.index;
                args.lastLogTerm = last_log.term;

                for (int i = 0; i < int(rpc_clients.size()); ++i) {
                    thread_pool->addObjJob(this, &raft::send_request_vote, i, args);
                    // send_request_vote(i, args);
                }

                // Change timeout randomly
                unsigned seed = 10086;
                srand(seed);
                timeout = (rand() % 300) + 200;
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    return;
}

template <typename state_machine, typename command> void raft<state_machine, command>::run_background_commit() {
    // Send logs/snapshots to the follower.
    // Only work for the leader.

    // Hints: You should check the leader's last log index and the follower's next log index.

    while (true) {
        if (is_stopped())
            return;
        // Your code here:
        if (role == leader) {
            int last = logs.back().index;
            for (int i = 0; i < num_nodes(); ++i) {
                if (i == my_id) {
                    continue;
                }
                if (nextIndex[i] <= last) {
                    if (nextIndex[i] > logs.front().index) {
                        append_entries_args<command> args;
                        args.term = current_term;
                        args.leaderId = my_id;
                        args.leaderCommit = commitIndex;
                        args.prevLogIndex = nextIndex[i] - 1;
                        args.prevLogTerm = logs.at(args.prevLogIndex - logs.front().index).term;
                        args.entries = getEntries(logs.front().index, last + 1);
                        thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
                    } else {
                        install_snapshot_args args;
                        args.term = current_term;
                        args.leaderId = my_id;
                        args.lastIncludedIndex = logs.front().index;
                        args.lastIncludedTerm = logs.front().term;
                        args.snapshot = snapshot;
                        thread_pool->addObjJob(this, &raft::send_install_snapshot, i, args);
                    }
                }
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    return;
}

template <typename state_machine, typename command> void raft<state_machine, command>::run_background_apply() {
    // Apply committed logs the state machine
    // Work for all the nodes.

    // Hints: You should check the commit index and the apply index.
    //        Update the apply index and apply the log if commit_index > apply_index

    while (true) {
        if (is_stopped())
            return;
        // Your code here:

        if (commitIndex > lastApplied) {
            std::vector<log_entry<command>> entries;
            entries = getEntries(lastApplied + 1, commitIndex + 1);
            for (log_entry<command> &entry : entries) {
                state->apply_log(entry.cmd);
            }
            lastApplied = commitIndex;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return;
}

template <typename state_machine, typename command> void raft<state_machine, command>::run_background_ping() {
    // Send empty append_entries RPC to the followers.

    // Only work for the leader.

    while (true) {
        if (is_stopped())
            return;
        // Your code here:

        if (role == leader) {
            append_entries_args<command> args;
            args.term = this->current_term;
            for (int i = 0; i < int(rpc_clients.size()); ++i) {
                thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
                // send_append_entries(i, args);
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(150)); // Change the timeout here!
    }
    return;
}

/******************************************************************

                        Other functions

*******************************************************************/

template <typename state_machine, typename command> void raft<state_machine, command>::setRole(raft_role r) {
    role = r;

    switch (role) {
    case leader: {
        RAFT_LOG("leader: %d", my_id)

        nextIndex.assign(num_nodes(), logs.back().index + 1);
        matchIndex.assign(num_nodes(), 0);
        matchIndex[my_id] = logs.back().index;
        matchCount.assign(logs.back().index - commitIndex, 0);

        break;
    }

    case follower: {
        break;
    }

    case candidate: {
        voteCount = 0;
        votedFor = -1;
        break;
    }

    default:
        break;
    }
}

template <typename state_machine, typename command>
inline std::vector<log_entry<command>> raft<state_machine, command>::getEntries(int begin_index, int end_index) const {
    std::vector<log_entry<command>> ret;
    if (begin_index < end_index) {
        ret.assign(logs.begin() + begin_index - logs.front().index, logs.begin() + end_index - logs.front().index);
    }
    return ret;
}

#endif // raft_h