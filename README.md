# Intro

The library support for auto scale & distritubed for Elixir application

## Guide

Just declare function for worker (task) and input (in param or stream) and run.

## Features

New kind of supervisor.

Chain processes.

Auto distributed processes.

## Supervisor

Support three type of processes in one supervisor. Can declare by config or add dynamic in runtime.

**Type of processes:**

- Group processes
- Chain processes
- Freedom processes

### Group processes

All processes in supervisor have same group_id.
If a process in group is crashed, all other processes will be died follow.
Avoid using trap_exit in process to avoid side effect.

### Chain processes

Support chain task type. The data after process in a process will be passed to next process in chain.
If a process is crashed, all other process in chain will be die follow.

### Freedom processes

Can add stand process to supervisor, this process run standalone.
If it's crashed, other processes in supervisor still run.
