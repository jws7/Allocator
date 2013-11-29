# Copyright (c) 2011-2012 OpenStack, LLC.
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

""" 

Modified by James Smith, jws7@st-andrews.ac.uk 
for investigation of efficient workload allocation algorithms.

Version 0.2 -- August 16th, 2013

"""

from nova.scheduler import filters
import MySQLdb as mdb
import sys
from datetime import datetime

class Allocator(filters.BaseHostFilter):
    """NOP host filter. Returns all hosts."""

    def custom_filter(self, host_state, filter_properties):
        con = None
        try:
            print "Connecting to DB"
            con = mdb.connect('host', 'username', 'password', 'db_name')
            cur = con.cursor()

            # Examine the scheduler_hints
            scheduler_hints = filter_properties.get('scheduler_hints') or {}
            vm_type = scheduler_hints.get('vm_type', [])

            # Insert filter request into FILTER table
            uuid = filter_properties['request_spec']['instance_properties']['uuid']
            request_id = uuid
            host_to_be_eval = host_state.host


            # Check if evalutation has been done or not
            print "Checking if rule evalation has been done..."
            cur.execute("SELECT complete FROM FILTER_EVALS WHERE uuid='" + str(uuid) + "'")
            score = cur.fetchone()

            if score == None:
                print "Evalution has not yet been completed for this uuid. Doing it now..."

                # VM Type is set by the user as a scheduler hint flag.
                print "Evaluting rules to place a " + vm_type + " instance"

                # Begin by opening the workload mix rules file
                f = open('/usr/lib/python2.7/dist-packages/nova/scheduler/filters/mix.rules', 'rU')

                # for each rule, evaluate it.
                for rule in f: 
                    print "\nEvaluating new rule..."
                    # Split rule into it's component parts
                    params = rule.split(":")

                    # If rule contains FILTER, it is a Filter rule and we should evaluate it here.
                    if "FILTER" in params:
                        print ".... FILTER rule."
                        print params

                        # Look for the job type
                        job = params[1]
                        if job.startswith("Job="):
                            job_type = job[4:]
                            print "for job_type: " + job_type
                            
                            # Is this rule applicable for the vm_type we are trying to create?
                            if job_type == vm_type:
                                print "Rule to be evaluated for this instance type"

                                # Look for the set rules
                                set_rule = params[2][1:-1]
                                set_rule_types = set_rule.split(",")
                                print "SET RULES TYPES:" + str(set_rule_types)

                                # Get all hosts in system (for now, it's two)
                                print "Getting all hosts in the system."
                                hosts = ["cloud-node-33", "cloud2-node1-46", "cloud-node-45", "cloud-node-36"] # TODO: replace with a mysql command

                                # Get hosts that match set_rules
                                print "Finding hosts that match set rules."
                                set_hosts = []

                                
                                for rule_type in set_rule_types:
                                    
                                    if rule_type == "EMPTY":
                                        # Deal with EMPTY hosts
                                        cur.execute("SELECT DISTINCT (host) FROM SCH_PLACEMENTS") # Returns all host that have vms
                                        results = cur.fetchall()
                                        
                                        # Put all hosts with VMs into a list
                                        loaded_hosts = []
                                        for host in results:
                                            loaded_hosts.append(host[0])
                                        
                                        # Take the list of hosts
                                        unloaded_hosts = hosts[:]
                                        # and remove those host which have VMs
                                        for host in loaded_hosts:
                                            unloaded_hosts.remove(host)
                                        # add theses hosts to the set hosts
                                        set_hosts = set_hosts + unloaded_hosts

                                    else:
                                        # For rules of a specific VM type
                                        # Get list of hosts with instances of this rule_type
                                        cur.execute("SELECT DISTINCT(host) FROM SCH_PLACEMENTS WHERE vm_type='" + str(rule_type) + "'")
                                        results = cur.fetchall()
                                        rule_hosts = []
                                        for host in results:
                                            rule_hosts.append(host[0])

                                        if len(rule_hosts) == 0:
                                            print "No hosts have instances of type: " + rule_type
                                        else:
                                            print "These hosts: " + str(rule_hosts) + " have the instances of the type " + rule_type

                                        set_hosts = set_hosts + rule_hosts
                                        print set_hosts
                 

                                # Now we have a list of all hosts that match the set rules.
                                # Calculate the length of the set
                                print "Calculating set len"
                                set_len = len(set_hosts)
                                print "Set length found to be " + str(set_len)

                                # Get the rule value
                                print "Checking set_length against rule..."
                                rule_var = int(params[3])
                                print "Var is " + str(rule_var)
                                
                                # If the set_len is greater than or equal to the parameter in the rule, complete the action
                                action_var = 0
                                if set_len >= rule_var:
                                    print "set_len meets or exeeds rule_var"
                                    action_var = 4 # Set match action
                                else:
                                    print "set rule is smaller than rule var"
                                    action_var = 5 # Alternate action

                                # Get action
                                action = params[action_var]
                                action = action.strip()

                                # Now we complete the action, whatever that might be.
                                if action == "PASS<SET>":
                                    print "So now passing all hosts in the set"
                                    # The most common action is to pass those hosts which met the set rules
                                    for host in set_hosts:
                                        print "passing host: " + str(host)

                                        # To pass the host, we now need to mark the host as suitable for this uuid in mysql
                                        cur.execute("INSERT INTO FILTER_DECISIONS VALUES ('" + str(uuid) + "', '" + str(host) + "', 1)")
                                        con.commit()

                                if action == "PASS<ALL>":
                                    print "So now passing all hosts"
                                    # The next common action is to pass all hosts
                                    for host in hosts:
                                        print "passing host: " + str(host)

                                        # To pass the host, we now need to mark the host as suitable for this uuid in mysql
                                        cur.execute("INSERT INTO FILTER_DECISIONS VALUES ('" + str(uuid) + "', '" + str(host) + "', 1)")
                                        con.commit()

                            else:
                                print "Rule not applicable for this instance type"

                        else:
                           print "No job paramater"
                    
                    if "WEIGH" in params:
                        print "....WEIGH rule."

                f.close()
                print "Evaluation of rules complete."

                # Now mark evaluation complete in mysql so it doesn't need to be computed again
                print "Marking completion of rule evaluation in db"
                # MYSQL INSERT INTO EVALS (uuid, 1)
                cur.execute("INSERT INTO FILTER_EVALS VALUES ('" + str(uuid) + "', 1)")
                con.commit()

            # All rules now evaluated.
            print "All rules evaluated for this instance request"

            # Get decision for this host.
            cur.execute("SELECT pass FROM FILTER_DECISIONS WHERE uuid='" + str(uuid) + "' AND host='" + str(host_to_be_eval) + "'")
            decision = cur.fetchone()
            if decision == None:
                print "This host (" + str(host_to_be_eval) + ") has NOT passed the filter"
                decision = 0
            else:
                print "This host (" + str(host_to_be_eval) + ") has passed the filter"
                decision = 1

            cur.execute("INSERT INTO REQs_FILTER(time, req_id, host, vm_type, passed) VALUES('" + str(datetime.now()) + "', '"  + str(request_id) + "', '" + str(host_state.host) + "', '" + str(vm_type) + "', " + str(decision) + ")")
            con.commit()

            if decision == 0:
                return False
            else:
                return True

        except mdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            sys.exit(1)
            return False
        finally:
            if con:
                con.close()

    def host_passes(self, host_state, filter_properties):
        return self.custom_filter(host_state, filter_properties)
        #return True
