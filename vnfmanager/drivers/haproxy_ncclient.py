# Copyright 2014 Tata Consultancy Services Ltd.
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

#from vnfmanager.commom import exceptions
import yaml
import time
import paramiko

from vnfmanager.drivers.templates_haproxy import haproxy_templates as templates
from ncclient import manager

class LoadBalancerDriver(object):
    def __init__(self , conf):
        self.conf = conf
        self.retries = 10
        self.webserver = self.conf['webserver']
        self.loadbalancer = self.conf['loadbalancer']
        try:
            for vdu in self.loadbalancer:
                if vdu['name'] == 'vLB':
                    self.lb_vdu = vdu
                    self.uname = vdu['vm_details']['image_details']['username']
                    self.password = vdu['vm_details']['image_details']['password']
                    break
        except KeyError:
            raise

    def get_type(self):
        """Return one of predefined type of the hosting device drivers."""
        pass

    def get_name(self):
        """Return a symbolic name for the service VM plugin."""
        pass

    def get_description(self):
        """Returns the description of driver"""
        pass

    def configure_service(self, *args):
        """configure the service """
        self.instance = self.lb_vdu['instance_list'][0]
        self.mgmt_ip = self.lb_vdu['mgmt-ip'][self.instance]
        self._check_connection()
        ipaddresses = []
        for vdu in self.webserver:
            if vdu['name'] == 'vAS':
                ips = vdu['vm_details']['network_interfaces']['pkt-in']['ips']
                for instance_name, ip in ips.iteritems():
                    ipaddresses.append(ip)
                    self.dev_name = instance_name
                break
        time.sleep(5)
        with manager.connect(host=self.mgmt_ip, username=self.uname, password=self.password, port=830, hostkey_verify=False) as m:
            confstr = templates.action.format(**{'action':'change'})
            m.edit_config(target='candidate', config=confstr)
            m.commit()
            confstr = templates.frontend_name.format(**{'name':'input'})
            m.edit_config(target='candidate', config=confstr)
            m.commit()
            confstr = templates.frontend_name.format(**{'name':'input'})
            m.edit_config(target='candidate', config=confstr)
            m.commit()
            confstr = templates.bind.format(**{'ip_port':'*:8080'})
            m.edit_config(target='candidate', config=confstr)
            m.commit()
            confstr = templates.default_backend.format(**{'backend_name':'output'})
            m.edit_config(target='candidate', config=confstr)
            m.commit()
            confstr = templates.backend_name.format(**{'backend_name':'output'})
            m.edit_config(target='candidate', config=confstr)
            m.commit()
            confstr = templates.balance.format(**{'balance_algorithm':'roundrobin'})
            m.edit_config(target='candidate', config=confstr)
            m.commit()
            confstr = templates.mode.format(**{'mode':'http'})
            m.edit_config(target='candidate', config=confstr)
            m.commit()
            for ipaddress in ipaddresses:
                confstr = templates.IPAddress.format(**{'IPAddress':ipaddress+':8080'})
                m.edit_config(target='candidate', config=confstr)
                m.commit()
            confstr = templates.action.format(**{'action':'restart'})
            m.edit_config(target='candidate', config=confstr)
            m.commit()
    
    def _check_connection(self):
        ssh_connected = False
        # keep connecting till ssh is success
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        while not ssh_connected:
            try:
                ssh.connect(self.mgmt_ip, username = self.uname, password = self.password, allow_agent=False, timeout=10)
                ssh_connected = True
            except Exception:
                time.sleep(5)
                pass


    def delete_service(self):
        """delete the service """
        pass

    def update_service(self):
        """update the service """
        pass
