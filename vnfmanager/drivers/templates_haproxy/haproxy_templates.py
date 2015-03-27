action = """
<config>
      <haproxy xmlns="http://netconfcentral.org/ns/haproxy">
        <backend>
          <action 
            xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0"
            nc:operation="replace">{action}</action>
        </backend>
      </haproxy>
    </config>
"""
frontend_name = """
<config>
      <haproxy xmlns="http://netconfcentral.org/ns/haproxy">
        <frontend>
          <name 
            xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0"
            nc:operation="replace">{name}</name>
        </frontend>
      </haproxy>
    </config>
"""
bind = """
<config>
      <haproxy xmlns="http://netconfcentral.org/ns/haproxy">
        <frontend>
          <bind 
            xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0"
            nc:operation="replace">{ip_port}</name>
        </frontend>
      </haproxy>
    </config>
"""
default_backend = """
<config>
      <haproxy xmlns="http://netconfcentral.org/ns/haproxy">
        <frontend>
          <default_backend 
            xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0"
            nc:operation="replace">{backend_name}</default_backend>
        </frontend>
      </haproxy>
</config>
"""
backend_name = """
<config>
      <haproxy xmlns="http://netconfcentral.org/ns/haproxy">
        <backend>
          <name 
            xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0"
            nc:operation="replace">{backend_name}</name>
        </backend>
      </haproxy>
</config>
"""
balance = """
<config>
      <haproxy xmlns="http://netconfcentral.org/ns/haproxy">
        <backend>
          <balance 
            xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0"
            nc:operation="replace">{balance_algorithm}</balance>
        </backend>
      </haproxy>
    </config>
"""
mode = """
<config>
      <haproxy xmlns="http://netconfcentral.org/ns/haproxy">
        <backend>
          <mode 
            xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0"
            nc:operation="replace">{mode}</mode>
        </backend>
      </haproxy>
    </config>
"""
IPAddress = """
<config>
      <haproxy xmlns="http://netconfcentral.org/ns/haproxy">
        <backend>
          <IPAddress 
            xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0"
            nc:operation="replace">{IPAddress}</IPAddress>
        </backend>
      </haproxy>
    </config>
"""
