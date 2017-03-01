// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */


#ifndef MGR_STANDBY_H_
#define MGR_STANDBY_H_

#include "auth/Auth.h"
#include "common/Finisher.h"
#include "common/Timer.h"

#include "DaemonServer.h"
#include "PyModules.h"

#include "DaemonState.h"
#include "ClusterState.h"

class Objecter;
class Client;

class MMgrMap;
class Mgr;

class MgrStandby : public Dispatcher {
protected:
  MonClient *monc;
  Objecter *objecter;
  std::unique_ptr<Client> client;
  Messenger *client_messenger;

  Mutex lock;
  SafeTimer timer;

  std::unique_ptr<Mgr> active_mgr;

  std::string state_str();

  void handle_mgr_map(MMgrMap *m);

public:
  MgrStandby();
  ~MgrStandby();

  bool ms_dispatch(Message *m);
  bool ms_handle_reset(Connection *con) { return false; }
  void ms_handle_remote_reset(Connection *con) {}
  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer,
                         bool force_new);
  bool ms_handle_refused(Connection *con);

  int init();
  void shutdown();
  void usage() {}
  int main(vector<const char *> args);
  void handle_signal(int signum);
  void send_beacon();
};

#endif

