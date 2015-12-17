// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 John Spray <john.spray@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef CEPH_PYFOO_H_
#define CEPH_PYFOO_H_

#include "osdc/Objecter.h"
#include "mds/MDSMap.h"
#include "messages/MMDSMap.h"
#include "msg/Dispatcher.h"
#include "msg/Messenger.h"
#include "auth/Auth.h"
#include "common/Finisher.h"


class PyFoo : public Dispatcher {
protected:
  Objecter *objecter;
  MDSMap *mdsmap;
  Messenger *messenger;
  MonClient *monc;

  Mutex lock;
  SafeTimer timer;
  Finisher finisher;

  Context *waiting_for_mds_map;

public:
  PyFoo();
  ~PyFoo();

  void handle_mds_map(MMDSMap* m);
  bool ms_dispatch(Message *m);
  bool ms_handle_reset(Connection *con) { return false; }
  void ms_handle_remote_reset(Connection *con) {}
  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer,
                         bool force_new);
  int init();
  void shutdown();
  void usage() {}
  int main(vector<const char *> args);
};

#endif /* MDS_UTILITY_H_ */
