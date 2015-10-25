// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */


#include "MDSUtility.h"

#include "include/rados/librados.hpp"

/**
 * When you want to let the user act on a single rank in a namespace,
 * or all of them.
 */
class MDSRoleSelector
{
  public:
    const std::vector<mds_role_t> &get_roles() const {return roles;}
    int parse(const MDSMap &mdsmap, std::string const &str);
    MDSRoleSelector()
      : ns(MDS_NAMESPACE_NONE)
    {}
    mds_namespace_t get_ns() const
    {
      return ns;
    }
  protected:
    int parse_rank(
        const MDSMap &mdsmap,
        std::string const &str);
    std::vector<mds_role_t> roles;
    mds_namespace_t ns;
};


/**
 * Command line tool for debugging the backing store of
 * MDSTable instances.
 */
class TableTool : public MDSUtility
{
  private:
    MDSRoleSelector role_selector;

    // I/O handles
    librados::Rados rados;
    librados::IoCtx io;

    int apply_role_fn(int (TableTool::*fptr) (mds_role_t, Formatter *), Formatter *f);

    int _reset_session_table(mds_role_t role, Formatter *f);
    int _show_session_table(mds_role_t role, Formatter *f);

    int _show_ino_table(mds_role_t role, Formatter *f);
    int _reset_ino_table(mds_role_t role, Formatter *f);

    int _show_snap_table(Formatter *f);
    int _reset_snap_table(Formatter *f);

  public:
    void usage();
    int main(std::vector<const char*> &argv);

};
