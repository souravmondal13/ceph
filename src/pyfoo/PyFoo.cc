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

// Python.h comes first because otherwise it clobbers ceph's assert
#include "Python.h"
// Python's pyconfig-64.h conflicts with ceph's acconfig.h
#undef HAVE_SYS_WAIT_H
#undef HAVE_UNISTD_H
#undef HAVE_UTIME_H
#undef _POSIX_C_SOURCE
#undef _XOPEN_SOURCE

#include "PyFoo.h"
#include "mon/MonClient.h"
#include "PyFormatter.h"


#include "global/global_context.h"



#define dout_subsys ceph_subsys_mon


PyFoo::PyFoo() :
  Dispatcher(g_ceph_context),
  objecter(NULL),
  lock("PyFoo::lock"),
  timer(g_ceph_context, lock),
  finisher(g_ceph_context, "PyFoo"),
  waiting_for_mds_map(NULL)
{
  monc = new MonClient(g_ceph_context);
  messenger = Messenger::create_client_messenger(g_ceph_context, "mds");
  mdsmap = new MDSMap();
  objecter = new Objecter(g_ceph_context, messenger, monc, NULL, 0, 0);
}


PyFoo::~PyFoo()
{
  delete objecter;
  delete monc;
  delete messenger;
  delete mdsmap;
  assert(waiting_for_mds_map == NULL);
}


int PyFoo::init()
{
  // Initialize Messenger
  int r = messenger->bind(g_conf->public_addr);
  if (r < 0)
    return r;

  messenger->start();

  objecter->set_client_incarnation(0);
  objecter->init();

  // Connect dispatchers before starting objecter
  messenger->add_dispatcher_tail(objecter);
  messenger->add_dispatcher_tail(this);

  // Initialize MonClient
  if (monc->build_initial_monmap() < 0) {
    objecter->shutdown();
    messenger->shutdown();
    messenger->wait();
    return -1;
  }

  monc->set_want_keys(CEPH_ENTITY_TYPE_MON|CEPH_ENTITY_TYPE_OSD|CEPH_ENTITY_TYPE_MDS);
  monc->set_messenger(messenger);
  monc->init();
  r = monc->authenticate();
  if (r < 0) {
    derr << "Authentication failed, did you specify an MDS ID with a valid keyring?" << dendl;
    monc->shutdown();
    objecter->shutdown();
    messenger->shutdown();
    messenger->wait();
    return r;
  }

  client_t whoami = monc->get_global_id();
  messenger->set_myname(entity_name_t::CLIENT(whoami.v));

  // Start Objecter and wait for OSD map
  objecter->start();
  objecter->wait_for_osd_map();
  timer.init();

  // Prepare to receive MDS map and request it
  Mutex init_lock("PyFoo:init");
  Cond cond;
  bool done = false;
  assert(!mdsmap->get_epoch());
  lock.Lock();
  waiting_for_mds_map = new C_SafeCond(&init_lock, &cond, &done, NULL);
  lock.Unlock();
  monc->sub_want("mdsmap", 0, CEPH_SUBSCRIBE_ONETIME);
  monc->renew_subs();

  // Wait for MDS map
  dout(4) << "waiting for MDS map..." << dendl;
  init_lock.Lock();
  while (!done)
    cond.Wait(init_lock);
  init_lock.Unlock();
  dout(4) << "Got MDS map " << mdsmap->get_epoch() << dendl;

  finisher.start();

  return 0;
}


void PyFoo::shutdown()
{
  finisher.stop();

  lock.Lock();
  timer.shutdown();
  objecter->shutdown();
  lock.Unlock();
  monc->shutdown();
  messenger->shutdown();
  messenger->wait();
}


bool PyFoo::ms_dispatch(Message *m)
{
   Mutex::Locker locker(lock);
   switch (m->get_type()) {
   case CEPH_MSG_MDS_MAP:
     handle_mds_map((MMDSMap*)m);
     break;
   case CEPH_MSG_OSD_MAP:
     break;
   default:
     return false;
   }
   return true;
}


void PyFoo::handle_mds_map(MMDSMap* m)
{
  mdsmap->decode(m->get_encoded());
  if (waiting_for_mds_map) {
    waiting_for_mds_map->complete(0);
    waiting_for_mds_map = NULL;
  }
}


bool PyFoo::ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer,
                         bool force_new)
{
  if (dest_type == CEPH_ENTITY_TYPE_MON)
    return true;

  if (force_new) {
    if (monc->wait_auth_rotating(10) < 0)
      return false;
  }

  *authorizer = monc->auth->build_authorizer(dest_type);
  return *authorizer != NULL;
}

static PyObject *global_pyhandle = NULL;

/* Return the number of arguments of the application command line */
static PyObject*
emb_handle(PyObject *self, PyObject *args)
{
  Py_INCREF(global_pyhandle);
  return global_pyhandle;
}

static PyMethodDef EmbMethods[] = {
    {"handle", emb_handle, METH_NOARGS,
     "Get the magic handle"},
    {NULL, NULL, 0, NULL}
};

int PyFoo::main(vector<const char *> args)
{
  // Construct the special python object from an MDSMap
  PyFormatter f;
  mdsmap->dump(&f);
  derr << "dumped!" << dendl;
  global_pyhandle = f.get();


  PyObject *dumped_mdsmap = f.get();
  Py_DECREF(dumped_mdsmap);

  PyObject *pName, *pModule, *pFunc;
  PyObject *pArgs, *pValue;

  Py_Initialize();

  Py_InitModule("emb", EmbMethods);

  // Construct pModule
  pName = PyString_FromString("foo");
  pModule = PyImport_Import(pName);
  Py_DECREF(pName);

  if (pModule != NULL) {
      pFunc = PyObject_GetAttrString(pModule, "multiply");
      /* pFunc is a new reference */

      if (pFunc && PyCallable_Check(pFunc)) {
          pArgs = PyTuple_New(2);

          pValue = PyInt_FromLong(12);
          PyTuple_SetItem(pArgs, 0, pValue);

          pValue = PyInt_FromLong(12);
          PyTuple_SetItem(pArgs, 1, pValue);

          pValue = PyObject_CallObject(pFunc, pArgs);
          Py_DECREF(pArgs);
          if (pValue != NULL) {
              printf("Result of call: %ld\n", PyInt_AsLong(pValue));
              Py_DECREF(pValue);
          } else {
              Py_DECREF(pFunc);
              Py_DECREF(pModule);
              PyErr_Print();
              return 1;
          }
      }
      else {
          if (PyErr_Occurred())
              PyErr_Print();
      }
      Py_XDECREF(pFunc);
      Py_DECREF(pModule);
  }
  else {
      PyErr_Print();
      return 1;
  }
  Py_Finalize();
  return 0;
}

