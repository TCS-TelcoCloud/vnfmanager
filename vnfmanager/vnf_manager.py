# Copyright (c) 2014 Tata Consultancy Services Limited(TCSL). 
# Copyright 2012 OpenStack Foundation
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import os
import sys
import threading
import time
import yaml
import itertools
import eventlet
eventlet.monkey_patch()

from oslo.config import cfg
from oslo import messaging

from vnfmanager import config
from vnfmanager import context
from vnfmanager import manager
from vnfmanager.openstack.common import loopingcall
from vnfmanager.openstack.common import service
from vnfmanager.openstack.common import log as logging
from vnfmanager.openstack.common.gettextutils import _

from vnfmanager.agent import rpc as agent_rpc
from vnfmanager.common import config as common_config
from vnfmanager.common import rpc as v_rpc
from vnfmanager.common import topics
from vnfmanager import service as vnfsvc_service
from vnfmanager.openstack.common import uuidutils as uuid
from vnfmanager.openstack.common import importutils
from vnfmanager.common import exceptions

LOG = logging.getLogger(__name__)

command_opts = [cfg.StrOpt('uuid', default=None, 
                help='VNF manager identifier'),
                cfg.StrOpt('vnfm-conf-dir', default=None,  
                help='VNF manager identifier')]
cfg.CONF.register_cli_opts(command_opts)

AGENT_VNF_MANAGER = 'VNF manager agent'

launchd_threads_info = dict()

class ImplThread(threading.Thread):
    def __init__(self, target, condition, *args, **kwargs):
        global launchd_threads_info
        self._id = kwargs['_id']
        self._condition = condition
        self._target = target
        self._args = args
        self._thr_info = launchd_threads_info
        threading.Thread.__init__(self)

    def run(self):
        try:
           self.return_vals = self._target(*self._args)
        except Exception:
           self.return_vals = 'Error'
        self._condition.acquire()
        self._thr_info[self._id]['result'] = self.return_vals
        self._condition.notify()
        self._condition.release()

class VNFManager(manager.Manager):

    def __init__(self, host=None):
        super(VNFManager, self).__init__(host=host)
        self.vplugin_rpc = VNFPluginCallbacks(topics.PLUGIN,
                                               cfg.CONF.host)
        self.needs_resync_reasons = []
        self.conf = cfg.CONF
        self.ctx = context.get_admin_context_without_session()
        self.ns_config = self.conf.vnfm_conf_d['service']
        self.drv_conf = self._extract_drivers()
        self.launched_devs = list()
        self._condition = threading.Condition()
        monitor_daemon = threading.Thread(target=self.monitor_thread_pool)
        monitor_daemon.setDaemon(True)
        LOG.warn(_("Waiter Daemon Starting"))
        self.configure_vdus(self.ctx)
        monitor_daemon.start()


    def monitor_thread_pool(self, thread_info=None, condition=None):
        global launchd_threads_info
        if thread_info is None:
            thread_info = launchd_threads_info
        if condition is None:
            condition = self._condition
        while True:
            condition.acquire()
            for each_thread in iter(thread_info.keys()):
                if 'result' in thread_info[each_thread].keys():
                    LOG.debug(_("Worker Thread # for VNF configuration Ending"), thread_info[each_thread])
                    LOG.debug(_("%s"), thread_info)
                    #print "Result:  "+str(thread_info[each_thread]['result'])+'\n'
                    ## TODO(padmaja, anirudh): The return value to be standardized, 
                    #                          to enable support crosss language drivers.
                    status = 'ERROR' if 'Error' in str(thread_info[each_thread]['result']) else 'COMPLETE' 
                    self.vplugin_rpc.send_ack(self.ctx,
                                               thread_info[each_thread]['vnfd'],
                                               thread_info[each_thread]['vdu'],
                                               thread_info[each_thread]['vm_name'],
                                               status,
                                               self.nsd_id)
                    if thread_info[each_thread]['thread_obj'].isAlive():
                        thread_info[each_thread]['thread_obj'].kill()
                    del(thread_info[each_thread])
            condition.wait()
            condition.release()


    def _extract_drivers(self):
        self.nsd_id = self.ns_config['nsd_id']
        vnfds = list(set(self.ns_config.keys()) - set(['id','nsd_id','fg']))
        vnfd_details = dict()
        for vnfd in vnfds:
            for vdu in range(0,len(self.ns_config[vnfd])):
                vdu_name = self.ns_config[vnfd][vdu]['name']
                vnfd_details[vdu_name] = dict()
                vnfd_details[vdu_name]['_instances'] = self.ns_config[vnfd][vdu]['instance_list']
                vnfd_details[vdu_name]['_driver'] = self.ns_config[vnfd][vdu]['mgmt-driver'] if self.ns_config[vnfd][vdu]['mgmt-driver'] is not '' else None
                vnfd_details[vdu_name]['_lc_events'] = self.ns_config[vnfd][vdu]['lifecycle_events']
                vnfd_details[vdu_name]['_vnfd'] = vnfd
                vnfd_details[vdu_name]['idx'] = vdu
        return vnfd_details


    def _populate_vnfd_drivers(self, drv_conf):
        vnfd_drv_cls_path = drv_conf['_driver']
        try:
            drv_conf['_drv_obj'] = importutils.import_object(vnfd_drv_cls_path,
                                                             self.ns_config)
        except Exception:
            LOG.warn(_("%s driver not Loaded"), vnfd_drv_cls_path)
            raise


    def _configure_service(self, vdu, instance):
        try:
            return vdu['_drv_obj'].configure_service(instance)

        except exceptions.DriverException or Exception:
            LOG.exception(_("Configuration of VNF Failed!!"))


    def _get_vdu_from_conf(self, conf):
        return conf[conf.keys()[0]][0]['name']


    def configure_vdus(self, context, conf=None):
        drv_conf = dict()
        new_drv_conf = list()

        if conf is not None:
            vnfds = list(set(conf['service'].keys()) - set(['nsd_id']))
            for vnfd in vnfds:
                if vnfd not in self.ns_config.keys():
                    self.ns_config[vnfd]= list()
            self.ns_config[vnfd].extend(conf['service'][vnfd])

        curr_drv_conf = self._extract_drivers()
        new_drv_conf = list(set(curr_drv_conf.keys()) - set(self.drv_conf.keys()))

        if new_drv_conf:
            drv_conf = curr_drv_conf
            self.drv_conf.update(curr_drv_conf)
        else:
            new_drv_conf = self.drv_conf.keys()
            drv_conf = self.drv_conf

        for vdu in new_drv_conf:
            if not drv_conf[vdu]['_driver']:
               continue
            self._populate_vnfd_drivers(drv_conf[vdu])

        for vdu_name in new_drv_conf:
            if not drv_conf[vdu_name]['_driver']:
                status = 'COMPLETE' 
                self.launched_devs.extend(drv_conf[vdu_name]['_instances'])
                for instance in drv_conf[vdu_name]['_instances']:
                    self.vplugin_rpc.send_ack(context, drv_conf[vdu_name]['_vnfd'],
                                              vdu_name,
                                              instance,
                                              status,
                                              self.nsd_id)
                continue
            else:
                status = self._configure_vdu(vdu_name)


    def _configure_vdu(self, vdu_name):
        status = ""
        for instance in range(0,len(self.drv_conf[vdu_name]['_instances'])):
            try:
                instance_name = self.drv_conf[vdu_name]['_instances'][instance]
                self._invoke_driver_thread(self.drv_conf[vdu_name],
                                           instance_name,
                                           vdu_name)
            except Exception:
                status = 'ERROR'
                LOG.warn(_("Configuration Failed for VNF %s"), instance_name)
                self.vplugin_rpc.send_ack(self.ctx,
                                          self.drv_conf[vdu_name]['_vnfd'],
                                          vdu_name,
                                          instance_name,
                                          status,
                                          self.nsd_id)


    def _invoke_driver_thread(self, vdu, instance, vdu_name):
        global launchd_threads_info
        LOG.debug(_("Configuration of the remote VNF %s being intiated"), instance)
        _thr_id = str(uuid.generate_uuid()).split('-')[1]   
        try:
           driver_thread = ImplThread(self._configure_service, self._condition, vdu, instance, _id = _thr_id)
           self._condition.acquire()
           launchd_threads_info[_thr_id] = {'vm_name': instance, 'thread_obj': driver_thread, 'vnfd': vdu['_vnfd'], 'vdu':vdu_name}
           self._condition.release()
           driver_thread.start()
        except RuntimeError:
           LOG.warning(_("Configuration by the Driver Failed!"))


class VNFPluginCallbacks(v_rpc.RpcProxy):
    """Manager side of the vnf manager to vnf Plugin RPC API."""

    def __init__(self, topic, host):
        RPC_API_VERSION = '1.0'
        super(VNFPluginCallbacks, self).__init__(topic, RPC_API_VERSION)

    def send_ack(self, context, vnfd, vdu, instance, status, nsd_id):
        return self.call(context,
                         self.make_msg('send_ack', vnfd=vnfd, vdu=vdu,
                                        instance=instance, status=status, nsd_id=nsd_id))


class VNFMgrWithStateReport(VNFManager):
    def __init__(self, host=None):
        super(VNFMgrWithStateReport, self).__init__(host=cfg.CONF.host)
        self.state_rpc = agent_rpc.PluginReportStateAPI(topics.PLUGIN)
        self.agent_state = {
            'binary': 'vnf-manager',
            'host': host,
            'topic': topics.set_topic_name(self.conf.uuid, prefix=topics.VNF_MANAGER),
            'configurations': {
                'agent_status': 'COMPLETE',
                'agent_id': cfg.CONF.uuid
                },
            'start_flag': True,
            'agent_type': AGENT_VNF_MANAGER}
        report_interval = 60                    # cfg.CONF.AGENT.report_interval. Hardcoded for time-while
                                                # However, they can be easily migrated to conf file.
        self.use_call = True
        if report_interval:
            self.heartbeat = loopingcall.FixedIntervalLoopingCall(
                self._report_state)
            self.heartbeat.start(interval=report_interval)


    def _report_state(self):
        try:
            self.agent_state.get('configurations').update(
                self.cache.get_state())
            ctx = context.get_admin_context_without_session()
            self.state_rpc.report_state(ctx, self.agent_state, self.use_call)
            #self.use_call = False
        except AttributeError:
            # This means the server does not support report_state
            LOG.warn(_("VNF server does not support state report."
                       " State report for this agent will be disabled."))
            self.heartbeat.stop()
            return
        except Exception:
            LOG.exception(_("Failed reporting state!"))
            return


def load_vnfm_conf(conf_path):
    conf_doc = open(conf_path, 'r')
    conf_dict = yaml.load(conf_doc)
    OPTS = [cfg.DictOpt('vnfm_conf_d', default=conf_dict)]
    cfg.CONF.register_opts(OPTS)


def _register_opts(conf):
    config.register_agent_state_opts_helper(conf)
    config.register_root_helper(conf)


def read_sys_args(arg_list):
    """ Reads a command-line arguments and returns a dict 
        for easier processing of cmd args and useful when 
        a number of args need to specified for the service
        without ordering them in a specific order. """
    arg_l = [arg if uuid.is_uuid_like(arg) else \
                             arg.lstrip('-').replace('-','_') for arg in arg_list[1:]]
    return dict(itertools.izip_longest(*[iter(arg_l)] * 2, fillvalue=""))


def main(manager='vnfmanager.vnf_manager.VNFMgrWithStateReport'):

    # placebo func to replace the server/__init__ with project startup. 
    # pool of threads needed to spawn worker threads for RPC.
    # Default action for project's startup, explictly maintainly a pool
    # for manager as it cannot inherit the vnfsvc's thread pool.
    pool = eventlet.GreenPool()
    pool.waitall()

    conf_params = read_sys_args(sys.argv)
    _register_opts(cfg.CONF)
    common_config.init(sys.argv[1:])
    uuid = conf_params['uuid']
    config.setup_logging(cfg.CONF)
    LOG.warn(_("UUID: %s"), uuid)
    vnfm_conf_dir = conf_params['vnfm_conf_dir'].split("/")
    vnfm_conf_dir[-2] = uuid
    vnfm_conf_dir = "/".join(vnfm_conf_dir)
    vnfm_conf_path = vnfm_conf_dir+uuid+'.yaml'
    load_vnfm_conf(vnfm_conf_path)
    server = vnfsvc_service.Service.create(
                binary='vnf-manager',
                topic=topics.set_topic_name(uuid, prefix=topics.VNF_MANAGER),
                report_interval=60, manager=manager)
    service.launch(server).wait()

