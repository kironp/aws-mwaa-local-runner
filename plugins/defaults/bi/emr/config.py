from defaults.bi.dag import LOGGING_S3_BUCKET


def get_emr_config(spark_job_name, spark_job_version, dag_id, env, spark_config=None, instance_config=None,
                   version=1):
    bucket = LOGGING_S3_BUCKET[env.lower()]

    job_flow_roles = {'data': 'roleBusinessIntelligenceEMR',
                      'staging': 'roleBusinessIntelligencePreProdEMR',
                      'dev': 'roleBusinessIntelligenceDevEMR'}
    emr_service_roles = {'data': 'roleTuringBIEMRService',
                         'staging': 'roleTuringBIEMRServicePreProd',
                         'dev': 'roleTuringBIEMRServiceDev'}

    job_flow_role = job_flow_roles[env.lower()]
    emr_service_role = emr_service_roles[env.lower()]

    if version == 1:
        emr_config = {
            "Instances": {
                "InstanceGroups": [],
                "KeepJobFlowAliveWhenNoSteps": True,
                "Ec2KeyName": "DataBI",
                "AdditionalMasterSecurityGroups": ["sg-6002bf07", "sg-6d8f8609"],
                "AdditionalSlaveSecurityGroups": ["sg-6d8f8609"],
                "Ec2SubnetId": "subnet-c781b39e"
            },
            "Configurations": [{
                "Properties": {
                    "hive.metastore.client.factory.class":
                        "com.amazonaws.glue.catalog.metastore."
                        "AWSGlueDataCatalogHiveClientFactory"
                },
                "Classification": "spark-hive-site"
            }, {
                "Classification": "spark-env",
                "Configurations": [{
                    "Classification": "export",
                    "Properties": {
                        "PYSPARK_PYTHON": "/home/hadoop/bi_spark_env/bin/python",
                        "TC_ENV": env.upper()
                    }
                }]
            }],
            "Tags": [{
                "Value": "datawarehouse-etl",
                "Key": "Role"
            }, {
                "Value": "Data",
                "Key": "EnvironmentType"
            }, {
                "Value": "BusinessIntelligence",
                "Key": "OwningCluster"
            }, {
                "Value": "KEEP",
                "Key": "TerminationPolicy"
            }, {
                "Value": "Secure",
                "Key": "SecurityZone"
            }, {
                "Value": "biteam@thetrainline.com",
                "Key": "ContactEmail"
            }, {
                "Value": "da1",
                "Key": "Environment"
            }, {
                "Value": "0.0.1",
                "Key": "emr-config-version"
            }, {
                "Value": spark_job_name,
                "Key": "Pipeline"
            }],
            "LogUri": "s3n://tld-prod-priv-data-lab/logs/emr/",
            "VisibleToAllUsers": True,
            "Applications": [{
                "Name": "Hadoop"
            }, {
                "Name": "Spark"
            }, {
                "Name": "Ganglia"
            }],
            "BootstrapActions": [{
                "ScriptBootstrapAction": {
                    "Path": "s3://tld-prod-priv-data-lab/bi_team/"
                            "bootstrap/bi_bootstrap_v1.sh",
                    "Args": ["bi_spark_env", spark_job_name, spark_job_version]
                },
                "Name": "Main_Bootstrap"
            }],
            "ServiceRole": "roleInfraAWSElasticMapReduce",
            "ReleaseLabel": "emr-5.28.0",
            "JobFlowRole": "roleBusinessIntelligenceEMR",
            "Name": {
                "prefix": "da1",
                "env": "prod",
                "app": dag_id,
                "label": "bi"
            }
        }
    elif version in [2, 3]:
        emr_release = {
            2: "emr-6.0.0",
            3: "emr-6.1.0"}

        emr_config = {
            "Instances": {
                "InstanceGroups": [],
                "KeepJobFlowAliveWhenNoSteps": False,
                "Ec2KeyName": "DataBI",
                "AdditionalMasterSecurityGroups": ["sg-6002bf07", "sg-6d8f8609"],
                "AdditionalSlaveSecurityGroups": ["sg-6d8f8609"],
                "Ec2SubnetId": "subnet-74d60502"
            },
            "Configurations": [{
                "Properties": {
                    "hive.metastore.client.factory.class":
                        "com.amazonaws.glue.catalog.metastore."
                        "AWSGlueDataCatalogHiveClientFactory"
                },
                "Classification": "spark-hive-site"
            }, {
                "Classification": "spark-env",
                "Configurations": [{
                    "Classification": "export",
                    "Properties": {
                        "TC_ENV": env.upper()
                    }
                }]
            }],
            "Tags": [{
                "Value": "datawarehouse-etl",
                "Key": "Role"
            }, {
                "Value": "Data",
                "Key": "EnvironmentType"
            }, {
                "Value": "BusinessIntelligence",
                "Key": "OwningCluster"
            }, {
                "Value": "KEEP",
                "Key": "TerminationPolicy"
            }, {
                "Value": "Secure",
                "Key": "SecurityZone"
            }, {
                "Value": "biteam@thetrainline.com",
                "Key": "ContactEmail"
            }, {
                "Value": "da1",
                "Key": "Environment"
            }, {
                "Value": "0.0.1",
                "Key": "emr-config-version"
            }, {
                "Value": spark_job_name,
                "Key": "Pipeline"
            }],
            "LogUri": "s3n://" + bucket + "/bi_team/logs/emr/" +
                      spark_job_name.strip() + "/" + spark_job_version.strip() + "/",
            "VisibleToAllUsers": True,
            "Applications": [{
                "Name": "Hadoop"
            }, {
                "Name": "Spark"
            }, {
                "Name": "Ganglia"
            }],
            "BootstrapActions": [{
                "ScriptBootstrapAction": {
                    "Path": "s3://" + bucket + "/bi_team/bootstrap/bi_bootstrap_v3.sh",
                    "Args": [spark_job_name, spark_job_version]
                },
                "Name": "Main_Bootstrap"
            }],
            "ServiceRole": "roleInfraAWSElasticMapReduce",
            "ReleaseLabel": emr_release[version],
            "JobFlowRole": job_flow_role,
            "Name": {
                "prefix": "da1",
                "env": env,
                "app": dag_id,
                "label": "bi"
            }
        }
    elif version == 4:
        emr_release = {
            4: "emr-6.0.0"}
        emr_config = {
            "Instances": {
                "InstanceFleets": [
                    {
                        "InstanceFleetType": "MASTER",
                        "TargetSpotCapacity": 1,
                        "InstanceTypeConfigs": instance_config['instance_type_master'],
                        "LaunchSpecifications": {
                            'SpotSpecification': {
                                'TimeoutDurationMinutes': 10,
                                'TimeoutAction': 'SWITCH_TO_ON_DEMAND',
                                'AllocationStrategy': 'capacity-optimized'
                            },
                            'OnDemandSpecification': {
                                'AllocationStrategy': 'lowest-price'
                            }
                        }

                    },
                    {
                        "Name": "Core nodes",
                        "InstanceFleetType": "CORE",
                        "TargetSpotCapacity": instance_config['instance_count'],
                        "InstanceTypeConfigs": instance_config['instance_type_core'],
                        "LaunchSpecifications": {
                            'SpotSpecification': {
                                'TimeoutDurationMinutes': 10,
                                'TimeoutAction': 'SWITCH_TO_ON_DEMAND',
                                'AllocationStrategy': 'capacity-optimized'
                            },
                            'OnDemandSpecification': {
                                'AllocationStrategy': 'lowest-price'
                            }
                        }
                    }

                ],
                "KeepJobFlowAliveWhenNoSteps": False,
                "Ec2KeyName": "DataBI",
                "AdditionalMasterSecurityGroups": ["sg-6002bf07", "sg-6d8f8609"],
                "AdditionalSlaveSecurityGroups": ["sg-6d8f8609"],
                "Ec2SubnetIds": ["subnet-74d60502", "subnet-63c63b07", "subnet-c781b39e"]
            },
            "Configurations": [{
                "Properties": {
                    "hive.metastore.client.factory.class":
                        "com.amazonaws.glue.catalog.metastore."
                        "AWSGlueDataCatalogHiveClientFactory"
                },
                "Classification": "spark-hive-site"
            }, {
                "Classification": "spark-env",
                "Configurations": [{
                    "Classification": "export",
                    "Properties": {
                        "TC_ENV": env.upper()
                    }
                }]
            },
                {"Classification": "spark-defaults",
                 "Properties": spark_config
                 }
            ],
            "Tags": [{
                "Value": "datawarehouse-etl",
                "Key": "Role"
            }, {
                "Value": "Data",
                "Key": "EnvironmentType"
            }, {
                "Value": "BusinessIntelligence",
                "Key": "OwningCluster"
            }, {
                "Value": "KEEP",
                "Key": "TerminationPolicy"
            }, {
                "Value": "Secure",
                "Key": "SecurityZone"
            }, {
                "Value": "biteam@thetrainline.com",
                "Key": "ContactEmail"
            }, {
                "Value": "da1",
                "Key": "Environment"
            }, {
                "Value": "0.0.1",
                "Key": "emr-config-version"
            }, {
                "Value": spark_job_name,
                "Key": "Pipeline"
            }],
            "LogUri": "s3n://" + bucket + "/bi_team/logs/emr/" +
                      spark_job_name.strip() + "/" + spark_job_version.strip() + "/",
            "VisibleToAllUsers": True,
            "Applications": [{
                "Name": "Hadoop"
            }, {
                "Name": "Spark"
            }, {
                "Name": "Ganglia"
            }],
            "BootstrapActions": [{
                "ScriptBootstrapAction": {
                    "Path": "s3://" + bucket + "/bi_team/bootstrap/bi_bootstrap_v3.sh",
                    "Args": [spark_job_name, spark_job_version]
                },
                "Name": "Main_Bootstrap"
            }],
            "ServiceRole": emr_service_role,
            "ReleaseLabel": emr_release[version],
            "JobFlowRole": job_flow_role,
            "Name": {
                "prefix": "da1",
                "env": env,
                "app": dag_id,
                "label": "bi"
            }
        }
    elif version == 5:
        emr_release = {
            5: "emr-6.1.0"}
        emr_config = {
            "Instances": {
                "InstanceFleets": [
                    {
                        "InstanceFleetType": "MASTER",
                        "TargetSpotCapacity": 1,
                        "InstanceTypeConfigs": instance_config['instance_type_master'],
                        "LaunchSpecifications": {
                            'SpotSpecification': {
                                'TimeoutDurationMinutes': 10,
                                'TimeoutAction': 'SWITCH_TO_ON_DEMAND',
                                'AllocationStrategy': 'capacity-optimized'
                            },
                            'OnDemandSpecification': {
                                'AllocationStrategy': 'lowest-price'
                            }
                        }

                    },
                    {
                        "Name": "Core nodes",
                        "InstanceFleetType": "CORE",
                        "TargetSpotCapacity": instance_config['instance_count'],
                        "InstanceTypeConfigs": instance_config['instance_type_core'],
                        "LaunchSpecifications": {
                            'SpotSpecification': {
                                'TimeoutDurationMinutes': 10,
                                'TimeoutAction': 'SWITCH_TO_ON_DEMAND',
                                'AllocationStrategy': 'capacity-optimized'
                            },
                            'OnDemandSpecification': {
                                'AllocationStrategy': 'lowest-price'
                            }
                        }
                    }

                ],
                "KeepJobFlowAliveWhenNoSteps": False,
                "Ec2KeyName": "DataBI",
                "AdditionalMasterSecurityGroups": ["sg-6002bf07", "sg-6d8f8609"],
                "AdditionalSlaveSecurityGroups": ["sg-6d8f8609"],
                "Ec2SubnetIds": ["subnet-74d60502", "subnet-63c63b07", "subnet-c781b39e"]
            },
            "Configurations": [{
                "Properties": {
                    "hive.metastore.client.factory.class":
                        "com.amazonaws.glue.catalog.metastore."
                        "AWSGlueDataCatalogHiveClientFactory"
                },
                "Classification": "spark-hive-site"
            }, {
                "Classification": "spark-env",
                "Configurations": [{
                    "Classification": "export",
                    "Properties": {
                        "TC_ENV": env.upper()
                    }
                }]
            },
                {"Classification": "spark-defaults",
                 "Properties": spark_config
                 }
            ],
            "Tags": [{
                "Value": "datawarehouse-etl",
                "Key": "Role"
            }, {
                "Value": "Data",
                "Key": "EnvironmentType"
            }, {
                "Value": "BusinessIntelligence",
                "Key": "OwningCluster"
            }, {
                "Value": "KEEP",
                "Key": "TerminationPolicy"
            }, {
                "Value": "Secure",
                "Key": "SecurityZone"
            }, {
                "Value": "biteam@thetrainline.com",
                "Key": "ContactEmail"
            }, {
                "Value": "da1",
                "Key": "Environment"
            }, {
                "Value": "0.0.1",
                "Key": "emr-config-version"
            }, {
                "Value": spark_job_name,
                "Key": "Pipeline"
            }],
            "LogUri": "s3n://" + bucket + "/bi_team/logs/emr/" +
                      spark_job_name.strip() + "/" + spark_job_version.strip() + "/",
            "VisibleToAllUsers": True,
            "Applications": [{
                "Name": "Hadoop"
            }, {
                "Name": "Spark"
            }, {
                "Name": "Ganglia"
            }],
            "BootstrapActions": [{
                "ScriptBootstrapAction": {
                    "Path": "s3://" + bucket + "/bi_team/bootstrap/bi_bootstrap_v3.sh",
                    "Args": [spark_job_name, spark_job_version]
                },
                "Name": "Main_Bootstrap"
            }],
            "ServiceRole": emr_service_role,
            "ReleaseLabel": emr_release[version],
            "JobFlowRole": job_flow_role,
            "Name": {
                "prefix": "da1",
                "env": env,
                "app": dag_id,
                "label": "bi"
            }
        }
    else:
        raise Exception('Unknown EMR Config Version')
    return emr_config
