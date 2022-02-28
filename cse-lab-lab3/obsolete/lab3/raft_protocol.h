#ifndef raft_protocol_h
#define raft_protocol_h

#include "raft_state_machine.h"
#include "rpc.h"

enum raft_rpc_opcodes { op_request_vote = 0x1212, op_append_entries = 0x3434, op_install_snapshot = 0x5656 };

enum raft_rpc_status { OK, RETRY, RPCERR, NOENT, IOERR };

class request_vote_args {
public:
    // Your code here
    int term;
    int candidateId;
    int lastLogIndex;
    int lastLogTerm;

    request_vote_args(){};
};

marshall &operator<<(marshall &m, const request_vote_args &args);
unmarshall &operator>>(unmarshall &u, request_vote_args &args);

class request_vote_reply {
public:
    // Your code here
    int term;
    bool voteGranted;

    request_vote_reply(){};
};

marshall &operator<<(marshall &m, const request_vote_reply &reply);
unmarshall &operator>>(unmarshall &u, request_vote_reply &reply);

template <typename command> class log_entry {
public:
    // Your code here
    log_entry() : term(0), index(0){};

    int term;
    int index;

    command cmd;
};

template <typename command> marshall &operator<<(marshall &m, const log_entry<command> &entry) {
    // Your code here
    m << entry.term;
    m << entry.index;
    m << entry.cmd;
    return m;
}

template <typename command> unmarshall &operator>>(unmarshall &u, log_entry<command> &entry) {
    // Your code here
    u >> entry.term;
    u >> entry.index;
    u >> entry.cmd;
    return u;
}

template <typename command> class append_entries_args {
public:
    // Your code here

    int term;
    int leaderId;
    int prevLogIndex;
    int prevLogTerm;
    int leaderCommit;

    std::vector<log_entry<command>> entries;

    append_entries_args(){};
};

template <typename command> marshall &operator<<(marshall &m, const append_entries_args<command> &args) {
    // Your code here
    m << args.term;
    m << args.leaderId;
    m << args.prevLogIndex;
    m << args.prevLogTerm;
    m << args.leaderCommit;

    m << int(args.entries.size());
    for (size_t i = 0; i < args.entries.size(); ++i) {
        m << args.entries.at(i);
    }
    return m;
}

template <typename command> unmarshall &operator>>(unmarshall &u, append_entries_args<command> &args) {
    // Your code here

    int size;

    u >> args.term;
    u >> args.leaderId;
    u >> args.prevLogIndex;
    u >> args.prevLogTerm;
    u >> args.leaderCommit;

    u >> size;

    args.entries.resize(size);
    for (size_t i = 0; i < size_t(size); ++i) {
        u >> args.entries[i];
    }

    return u;
}

class append_entries_reply {
public:
    // Your code here
    int term;
    bool success;

    append_entries_reply(){};
};

marshall &operator<<(marshall &m, const append_entries_reply &reply);
unmarshall &operator>>(unmarshall &u, append_entries_reply &reply);

class install_snapshot_args {
public:
    // Your code here
    int term;
    int leaderId;
    int lastIncludedIndex;
    int lastIncludedTerm;
    std::vector<char> snapshot;

    install_snapshot_args(){};
};

marshall &operator<<(marshall &m, const install_snapshot_args &args);
unmarshall &operator>>(unmarshall &u, install_snapshot_args &args);

class install_snapshot_reply {
public:
    // Your code here
    int term;

    install_snapshot_reply() : term(0){};
};

marshall &operator<<(marshall &m, const install_snapshot_reply &reply);
unmarshall &operator>>(unmarshall &u, install_snapshot_reply &reply);

#endif // raft_protocol_h