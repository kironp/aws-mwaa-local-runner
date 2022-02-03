emr_template = {
    'ReleaseLabel': None,
    'BootstrapActions': [],
    'Name': None,
    'StepConcurrencyLevel': None,
    'Steps': [],
    'LogUri': None,
    'Instances': {
        'InstanceFleets': [],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
        'Ec2KeyName': 'DataBI',
        'AdditionalMasterSecurityGroups': ['sg-6002bf07',
                                           'sg-6d8f8609'],
        'AdditionalSlaveSecurityGroups': ['sg-6d8f8609'],
        'Ec2SubnetIds': ['subnet-74d60502',
                         'subnet-63c63b07',
                         'subnet-c781b39e']},
    'Configurations': [],
    'Tags': [
        {'Value': 'datawarehouse-etl', 'Key': 'Role'},
        {'Value': 'Data', 'Key': 'EnvironmentType'},
        {'Value': 'BusinessIntelligence', 'Key': 'OwningCluster'},
        {'Value': 'KEEP', 'Key': 'TerminationPolicy'},
        {'Value': 'Secure', 'Key': 'SecurityZone'},
        {'Value': 'biteam@thetrainline.com',
         'Key': 'ContactEmail'},
        {'Value': 'da1', 'Key': 'Environment'}
    ],

    'VisibleToAllUsers': True,
    'Applications': [
        {'Name': 'Spark'},
        {'Name': 'Ganglia'}
    ],
    'ServiceRole': None,
    'JobFlowRole': None
}

instance_fleet_template_master = {
    'Name': 'Master nodes',
    'InstanceFleetType': 'MASTER',
    'TargetOnDemandCapacity': 1,
    'InstanceTypeConfigs': [],
    'LaunchSpecifications': {
        'OnDemandSpecification': {
            'AllocationStrategy': 'lowest-price'}}
}

instance_fleet_template_core = {
    'Name': 'Core nodes',
    'InstanceFleetType': 'CORE',
    'TargetSpotCapacity': None,
    'InstanceTypeConfigs': [],
    'LaunchSpecifications': {
        'SpotSpecification': {
            'TimeoutDurationMinutes': 10,
            'TimeoutAction': 'SWITCH_TO_ON_DEMAND',
            'AllocationStrategy': 'capacity-optimized'},
        'OnDemandSpecification': {
            'AllocationStrategy': 'lowest-price'}}
}

emr_master_instance_config = [
    {
        "InstanceType": "m4.2xlarge",
        "WeightedCapacity": 1,
        "BidPrice": "0.595"
    },
    {
        "InstanceType": "m5.2xlarge",
        "WeightedCapacity": 1,
        "BidPrice": "0.454"
    },
    {
        "InstanceType": "m5a.2xlarge",
        "WeightedCapacity": 1,
        "BidPrice": "0.384"
    }
]
