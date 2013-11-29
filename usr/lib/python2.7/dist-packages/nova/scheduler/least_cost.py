# Copyright (c) 2011 OpenStack, LLC.
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
Least Cost is an algorithm for choosing which host machines to
provision a set of resources to. The input is a WeightedHost object which
is decided upon by a set of objective-functions, called the 'cost-functions'.
The WeightedHost contains a combined weight for each cost-function.

The cost-function and weights are tabulated, and the host with the least cost
is then selected for provisioning.

edited by James Smith, University of St Andrews 2013.

"""

from nova import flags
from nova import log as logging
from nova.openstack.common import cfg
import MySQLdb as mdb
import sys
import random
from datetime import datetime

LOG = logging.getLogger(__name__)

least_cost_opts = [
    cfg.ListOpt('least_cost_functions',
                default=[
                  'nova.scheduler.least_cost.compute_fill_first_cost_fn'
                  ],
                help='Which cost functions the LeastCostScheduler should use'),
    cfg.FloatOpt('noop_cost_fn_weight',
             default=1.0,
               help='How much weight to give the noop cost function'),
    cfg.FloatOpt('compute_fill_first_cost_fn_weight',
             default=-1.0,
               help='How much weight to give the fill-first cost function. '
                    'A negative value will reverse behavior: '
                    'e.g. spread-first'),
    ]

FLAGS = flags.FLAGS
FLAGS.register_opts(least_cost_opts)

# TODO(sirp): Once we have enough of these rules, we can break them out into a
# cost_functions.py file (perhaps in a least_cost_scheduler directory)


class WeightedHost(object):
    """Reduced set of information about a host that has been weighed.
    This is an attempt to remove some of the ad-hoc dict structures
    previously used."""

    def __init__(self, weight, host_state=None):
        self.weight = weight
        self.host_state = host_state

    def to_dict(self):
        x = dict(weight=self.weight)
        if self.host_state:
            x['host'] = self.host_state.host
        return x


def noop_cost_fn(host_state, weighing_properties):
    """Return a pre-weight cost of 1 for each host"""
    return 1

"""
def compute_fill_first_cost_fn(host_state, weight_properties):
    Higher weights win, so we should return a lower weight
    when there's more free ram available.

    Note: the weight modifier for this function in default configuration
    is -1.0. With -1.0 this function runs in reverse, so systems
    with the most free memory will be preferred.
    return -host_state.free_ram_mb
"""

def allocator_cost_fn(host_state, weighing_properties):
    
    con = None
    try:
        print "Connecting to DB"
        con = mdb.connect('host', 'username', 'password', 'db_name')
        cur = con.cursor()
        
        # Examine the scheduler_hints
        scheduler_hints = weighing_properties.get('scheduler_hints') or {}
        vm_type = scheduler_hints.get('vm_type', [])
        
        # Insert weigh request into WEIGH table
        request_id = weighing_properties['request_spec']['instance_properties']['uuid']
        uuid = request_id
        host_to_be_eval = host_state.host

        # Begin calculation of scores for each host (if not done before)
        # Check if evalutation has been done or not
        print "Checking if WEIGH evalation has been done..."
        cur.execute("SELECT complete FROM WEIGH_EVALS WHERE uuid='" + str(uuid) + "'")
        score = cur.fetchone()

        if score == None:
            print "WEIGH Evalution has not yet been completed for this uuid. Doing it now..."

            # Get list of hosts that passed the filter
            cur.execute("SELECT host FROM FILTER_DECISIONS WHERE uuid='" + str(uuid) + "'")
            results = cur.fetchall()
            passed_filter_hosts = []
            for host in results:
                passed_filter_hosts.append(host[0])

            # Get list current hosts with VMs
            cur.execute("SELECT DISTINCT(host) FROM SCH_PLACEMENTS")
            results = cur.fetchall()
            loaded_hosts = []
            for host in results:
                loaded_hosts.append(host[0])

            # and list of vms without hosts
            unloaded_hosts = passed_filter_hosts[:]
            #
            for loaded_host in loaded_hosts:
                if loaded_host in unloaded_hosts:
                    unloaded_hosts.remove(loaded_host)

            # Setup scores for each host
            score_dict = {}
            for host in passed_filter_hosts:
                score_dict[host] = 0
                # Evaluate a score for each host

                # If host is empty, score = 16
                if host in unloaded_hosts:
                    score_dict[host] = 16        
   
                # If host has one instance of same vm_type, score = 20
                cur.execute("SELECT count(*) FROM SCH_PLACEMENTS WHERE host = '" + str(host) + "' AND vm_type = '" + str(vm_type) + "'")
                count = cur.fetchone()
                count = int(count[0])
                if count >= 1:
                    score_dict[host] = 20

                # Remove 2 from score for each additional vm of same type
                if count > 1:
                    #raise Exception ("count = " + str(count))
                    penalty = count - 1
                    if vm_type == "CPU":
                        penalty = penalty * 1 # remove only one point for CPU jobs
                    else:
                        penalty = penalty * 2
                    score_dict[host] = score_dict[host] - penalty

                # Remove 5 for different type vms on each host
                cur.execute("SELECT count(*) FROM SCH_PLACEMENTS WHERE host = '" + str(host) + "'")
                total_count = cur.fetchone()
                total_count = int(total_count[0])
                if total_count > count:
                    penalty = total_count - count
                    penalty = penalty * 5
                    score_dict[host] = score_dict[host] - penalty

                # Add a small random value to each score to avoid conflicts
                random_var = random.uniform(0, 0.1)
                score_dict[host] = score_dict[host] + random_var

            # Record scores in db
            for k, v in score_dict.items():
                #raise Exception ("score_dict: " + str(score_dict) + " AND unloaded_hosts: " + str(unloaded_hosts))
                command = "INSERT INTO WEIGH_DECISIONS(uuid, host, score) VALUES('" + str(uuid) + "', '" + str(k) + "', " + str(v) + ")"
                cur.execute(command)
                con.commit()
                    
            # Mark weigh evaluations as complete
            cur.execute("INSERT INTO WEIGH_EVALS(uuid, complete) VALUES('" + str(request_id) + "', 1)")
            con.commit()

        # If all calculations done, return score for the host
        # All rules now evaluated.
        print "Weigh evaluated for this instance request"

        # Get decision for this host.
        cur.execute("SELECT score FROM WEIGH_DECISIONS WHERE uuid='" + str(uuid) + "' AND host='" + str(host_to_be_eval) + "'")
        decision = cur.fetchone()
        decision = float(decision[0])
        cost = decision
        
        # Insert request to weigh
        cur.execute("INSERT INTO REQs_WEIGH(time, req_id, host, cost, vm_type) VALUES('" + str(datetime.now()) + "', '"  + str(request_id) + "', '" + str(host_state.host) + "', '" + str(cost) + "', '" + str(vm_type) + "')")
        con.commit()

        # Update SCH_PLACEMENTS based on lowest cost host
        cur.execute("SELECT cost FROM SCH_PLACEMENTS WHERE req_id='" + str(request_id) + "'")
        data = cur.fetchone()
        if data: 
            # Compare current cost to this cost and update if better (remembering also to update the host)
            cur_cost = float(data[0])
            if cost > cur_cost:
                cur.execute("UPDATE SCH_PLACEMENTS SET cost=" + str(cost) + ", host='" + str(host_state.host) + "' WHERE req_id='" + str(request_id) + "'")
                con.commit()

        else:
            # There is no current placement entry for this req_id
            cur.execute("INSERT INTO SCH_PLACEMENTS(time, req_id, host, cost, vm_type) VALUES('" + str(datetime.now()) + "', '"  + str(request_id) + "', '" + str(host_state.host) + "', '" + str(cost) + "', '"+ str(vm_type) +"')")           
            con.commit()

    except mdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        sys.exit(1)
        return False
    finally:
        if con:
            con.close()
    return cost


def weighted_sum(weighted_fns, host_states, weighing_properties):
    """Use the weighted-sum method to compute a score for an array of objects.

    Normalize the results of the objective-functions so that the weights are
    meaningful regardless of objective-function's range.

    :param host_list:    ``[(host, HostInfo()), ...]``
    :param weighted_fns: list of weights and functions like::

        [(weight, objective-functions), ...]

    :param weighing_properties: an arbitrary dict of values that can
        influence weights.

    :returns: a single WeightedHost object which represents the best
              candidate.
    """

    # Make a grid of functions results.
    # One row per host. One column per function.
    scores = []
    for weight, fn in weighted_fns:
        scores.append([fn(host_state, weighing_properties)
                for host_state in host_states])

    # Adjust the weights in the grid by the functions weight adjustment
    # and sum them up to get a final list of weights.
    adjusted_scores = []
    for (weight, fn), row in zip(weighted_fns, scores):
        adjusted_scores.append([weight * score for score in row])

    # Now, sum down the columns to get the final score. Column per host.
    final_scores = [0.0] * len(host_states)
    for row in adjusted_scores:
        for idx, col in enumerate(row):
            final_scores[idx] += col

    # Super-impose the host_state into the scores so
    # we don't lose it when we sort.
    final_scores = [(final_scores[idx], host_state)
            for idx, host_state in enumerate(host_states)]

    final_scores = sorted(final_scores)
    weight, host_state = final_scores[0]  # Lowest score is the winner!
    return WeightedHost(weight, host_state=host_state)
