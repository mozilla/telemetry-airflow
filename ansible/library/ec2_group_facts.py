#!/usr/bin/python
#
# Copied from ansible-modules-extras.
#
# This is a free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This Ansible library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this library.  If not, see <http://www.gnu.org/licenses/>.

DOCUMENTATION = '''
---
module: ec2_group_facts
short_description: Gather facts about security groups in AWS
description:
    - Gather facts about security groups in AWS
version_added: "2.2"
author: "Destry Jaimes (@calamityman)"
options:
  filters:
    description:
      - A dict of filters to apply. Each dict item consists of a filter key and a filter value. See U(http://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeSecurityGroups.html) for possible filters.
    required: false
    default: null
    
extends_documentation_fragment:
    - aws
    - ec2
'''

EXAMPLES = '''
# Note: These examples do not set authentication details, see the AWS Guide for details.

# Gather facts about all security groups
- ec2_group_facts:

# Gather facts about security groups in a particular VPC using VPC ID
- ec2_group_facts:
    filters:
      vpc-id: vpc-12345678

# Gather facts about any security group with a tag key Name and value Example
- ec2_group_facts:
    filters:
      "tag:Name": Example

'''

RETURN = '''
description:
    description: Security group description
    type: string
    sample: My security group description
group_id:
    description: Security Group ID
    type: string
    sample: sg-12345678
group_name:
    description: Security group name
    type: string
    sample: My AWS Security Group
ip_permissions:
    description: Ingress rules associated with the security group. See U(https://boto3.readthedocs.org/en/latest/reference/services/ec2.html#securitygroup) for more info.
    type: list
    contains:
        ip_protocol:
            description: The IP protocol name (for tcp , udp , and icmp ) or number
            type: string
        from_port:
            description: The start of port range for the TCP and UDP protocols, or an ICMP type number. A value of -1 indicates all ICMP types.
            type: int
        to_port:
            description: The end of port range for the TCP and UDP protocols, or an ICMP code. A value of -1 indicates all ICMP codes for the specified ICMP type.
            type: int
        user_id_group_pairs:
            description: One or more security group and AWS account ID pairs.
            type: list
            contains:
                user_id: 
                    description: ID of an AWS acccount
                    type: string
                group_name:
                    description: The name of the security group.
                    type: string
                group_id:
                    description: The ID of the security group.
                    type: string
                vpc_id:
                    description: The ID of the VPC for the referenced security group, if applicable.
                    type: string
                vpc_peering_connection_id:
                    description: The ID of the VPC peering connection, if applicable.
                    type: string
                peering_status:
                    description: The status of a VPC peering connection, if applicable.
                    type: string
        ip_ranges:
            description: One or more IP ranges.
            type: list
            contains:
                cidr_ip:
                    description: The CIDR range. You can either specify a CIDR range or a source security group, not both.
                    type: string
        prefix_list_ids:
            description: One or more prefix list IDs for an AWS service. 
            type: list
            contains:
                prefix_list_id:
                    description: The ID of the prefix.
                    type: string
ip_permissions_egress:
    description: One or more outbound rules associated with the security group. See U(https://boto3.readthedocs.org/en/latest/reference/services/ec2.html#securitygroup) for more info.
    type: list
    contains:
        ip_protocol:
            description: The IP protocol name (for tcp , udp , and icmp ) or number
            type: string
        from_port:
            description: The start of port range for the TCP and UDP protocols, or an ICMP type number. A value of -1 indicates all ICMP types.
            type: int
        to_port:
            description: The end of port range for the TCP and UDP protocols, or an ICMP code. A value of -1 indicates all ICMP codes for the specified ICMP type.
            type: int
        user_id_group_pairs:
            description: One or more security group and AWS account ID pairs.
            type: list
            contains:
                user_id: 
                    description: ID of an AWS acccount
                    type: string
                group_name:
                    description: The name of the security group.
                    type: string
                group_id:
                    description: The ID of the security group.
                    type: string
                vpc_id:
                    description: The ID of the VPC for the referenced security group, if applicable.
                    type: string
                vpc_peering_connection_id:
                    description: The ID of the VPC peering connection, if applicable.
                    type: string
                peering_status:
                    description: The status of a VPC peering connection, if applicable.
                    type: string
        ip_ranges:
            description: One or more IP ranges.
            type: list
            contains:
                cidr_ip:
                    description: The CIDR range. You can either specify a CIDR range or a source security group, not both.
                    type: string
        prefix_list_ids:
            description: One or more prefix list IDs for an AWS service. 
            type: list
            contains:
                prefix_list_id:
                    description: The ID of the prefix.
                    type: string
owner_id:
    description: AWS account ID of the security group owner
    type: string
    sample: 012345678901
tags:
    description: Any tags assigned to the security group
    type: list
    contains:
        key:
            description: The key of the tag.
            type: string
        value:
            description: The value of the tag.
            type: string
vpc_id:
    description: ID of the VPC for the security group
    type: string
    sample: vpc-12345678

'''

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False


def list_ec2_groups(connection, module):

    group_ids = module.params.get("group_ids")
    group_names = module.params.get("group_names")
    filters = ansible_dict_to_boto3_filter_list(module.params.get("filters"))

    try:
        all_sgs = connection.describe_security_groups(GroupIds=group_ids, GroupNames=group_names, Filters=filters)
    except ClientError as e:
        module.fail_json(msg=e.message)

    # turn the boto3 result into ansible friendly snaked names
    snaked_groups = []
    for group in all_sgs['SecurityGroups']:
        snaked_groups.append(camel_dict_to_snake_dict(group))

    # turn the boto3 result into ansible friendly tag dictionary
    for group in snaked_groups:
        if 'Tags' in group:
            group['Tags'] = boto3_tag_list_to_ansible_dict(group['Tags'])

    module.exit_json(sgs=snaked_groups)


def main():

    argument_spec = ec2_argument_spec()
    argument_spec.update(
        dict(
                group_ids=dict(default=[], type='list'),
                group_names=dict(default=[], type='list'),
                filters=dict(default={}, type='dict')
        )
    )

    module = AnsibleModule(argument_spec=argument_spec,
                           mutually_exclusive=[
                               ['group_ids', 'filters']
                           ]
                           )

    if not HAS_BOTO3:
        module.fail_json(msg='boto3 required for this module')

    region, ec2_url, aws_connect_params = get_aws_connection_info(module, boto3=True)

    if region:
        connection = boto3_conn(module, conn_type='client', resource='ec2', region=region, endpoint=ec2_url, **aws_connect_params)
    else:
        module.fail_json(msg="region must be specified")

    list_ec2_groups(connection, module)

from ansible.module_utils.basic import *
from ansible.module_utils.ec2 import *

if __name__ == '__main__':
    main()
