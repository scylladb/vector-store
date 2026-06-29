#!/usr/bin/python3

# Copyright 2025-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import os
import sys
import shlex
import argparse
import subprocess

HOME_DIR='/home/ubuntu'
USER='ubuntu'
COPY_OR_MOVE='mv'

def run(template_cmd, shell=False, ignoer_error=True):
    cmd = template_cmd.format(_HOME=HOME_DIR, _USER=USER, _COPY_OR_MOVE=COPY_OR_MOVE)
    if dry_run:
        print(cmd)
        return
    if verbose:
        print(cmd)
    if not shell:
        cmd = shlex.split(cmd)
    try:
        res = subprocess.check_output(cmd, shell=shell)
        if verbose:
            print(res)
        return res
    except Exception as e:
        print("Error while running:")
        print(cmd,end =" ")
        print(e.output)
        if not ignoer_error:
            raise

def chdir(template_dir):
    dir = template_dir.format(_HOME=HOME_DIR, _USER=USER)
    if dry_run:
        print('cd ', dir)
        return
    if verbose:
        print('cd ', dir)
    os.chdir(dir)

def install_vector_search(version, arch):
    name = 'vector-search-{VERSION}-{ARC}'.format(VERSION=version, ARC=arch)
    run('sudo -u {_USER} curl -L -o {_HOME}/vector-search.tar.gz  https://github.com/scylladb/vector-search/releases/download/{VERSION}/{NAME}.tar.gz'.format(VERSION=version, _HOME=HOME_DIR, _USER=USER, NAME=name))
    run('sudo -u {_USER} tar -xvf {_HOME}/vector-search.tar.gz -C {_HOME}/')
    run('sudo -u {_USER} {_COPY_OR_MOVE} {_HOME}/{NAME} {_HOME}/vector-search'.format(NAME=name, _HOME=HOME_DIR, _USER=USER, _COPY_OR_MOVE=COPY_OR_MOVE))

def install_node_exporter(arch):
    try:
        run('{_HOME}/node_exporter_install --arch {arch}'.format(arch=arch, _HOME=HOME_DIR,))
    except Exception as e:
        print("node_exporter_install failed" + str(e))
        print(e.output)
    try:
        run('cp {_HOME}/node-exporter.service /usr/lib/systemd/system/')
    except Exception as e:
        print("could not place node-exporter file in user/lib/systemd" + str(e))
        print(e.output)
    run('systemctl daemon-reload')
    run('systemctl enable node-exporter.service')
    run('systemctl start node-exporter.service')

def install_service():
    run('cp {_HOME}/vector-search.service /etc/systemd/system/')
    try:
        run('cp {_HOME}/vector-search.service /usr/lib/systemd/system/')
    except Exception as e:
        print("could not place vector-search file in user/lib/systemd" + str(e))
        print(e.output)
    run('systemctl daemon-reload')

def install_docker():
    run('./install-docker.sh')

def install_monitoring(version):
    if version == 'master':
        run('sudo -u {_USER} curl -L -o {_HOME}/scylla-monitoring.tar.gz  https://github.com/scylladb/scylla-monitoring/archive/refs/heads/master.tar.gz'.format(_HOME=HOME_DIR, _USER=USER))
        name = 'scylla-monitoring-master'
    else:
        run('sudo -u {_USER} curl -L -o {_HOME}/scylla-monitoring.tar.gz  https://github.com/scylladb/scylla-monitoring/archive/refs/tags/{VERSION}.tar.gz'.format(VERSION=version, _HOME=HOME_DIR, _USER=USER))
        name = 'scylla-monitoring-{VERSION}'.format(VERSION=version)
    run('sudo -u {_USER} tar -xvf {_HOME}/scylla-monitoring.tar.gz -C {_HOME}/')
    run('sudo -u {_USER} {_COPY_OR_MOVE} {_HOME}/{NAME} {_HOME}/scylla-monitoring'.format(NAME=name, _HOME=HOME_DIR, _USER=USER, _COPY_OR_MOVE=COPY_OR_MOVE))

def install_client():
    run('sudo -u {_USER} curl -L -o {_HOME}/VectorDBBench.tar.gz  https://github.com/scylladb/VectorDBBench/archive/refs/heads/main.tar.gz'.format(_HOME=HOME_DIR, _USER=USER))
    run('sudo -u {_USER} tar -xvf {_HOME}/VectorDBBench.tar.gz -C {_HOME}/')
    run('sudo -u {_USER} {_COPY_OR_MOVE} {_HOME}/VectorDBBench-main {_HOME}/VectorDBBench'.format(_HOME=HOME_DIR, _USER=USER, _COPY_OR_MOVE=COPY_OR_MOVE))
    run('sudo -u {_USER} {_HOME}/clientpy.sh'.format(_USER=USER, _HOME=HOME_DIR))

if __name__ == '__main__':
    if os.getuid() > 0:
        print('Requires root permission.')
        sys.exit(1)
    parser = argparse.ArgumentParser(description='General installer for cluster_test roles')
    parser.add_argument('--version', default="0.1.0", help='version to install')
    parser.add_argument('--arch', default="amd64", help='Architecture to use (amd64/arm64), currently only amd64 is supported')
    parser.add_argument('--cloud', default="aws", choices=['aws','gce'], help='What cloud we are running on, currently only aws is supported')
    parser.add_argument('--verbose', action='store_true', default=False, help='Verbose trace mode')
    parser.add_argument('--do-not-move', action='store_true', default=False, help='For testing, do not move files, copy them instead')
    parser.add_argument('-d', '--dry-run', action='store_true', default=False, help='Dry run mode')
    parser.add_argument('app', choices=['vector-search', 'node-exporter', 'monitor', 'docker', 'client'], help='What to install')

    args = parser.parse_args()
    dry_run = args.dry_run
    verbose = args.verbose

    if args.do_not_move:
        COPY_OR_MOVE='cp'
    if args.app == 'vector-search':
        install_vector_search(args.version, args.arch)
        install_service()
    elif args.app == 'node-exporter':
        install_node_exporter(arch=args.arch)
    elif args.app == 'monitor':
        install_monitoring(args.version)
    elif args.app == 'docker':
        install_docker()
    elif args.app == 'client':
        install_client()