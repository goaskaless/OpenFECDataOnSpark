############################################
# INITIALIZE LAB PARAMETERS AND VARIABLES  #
############################################
CLIENT_IP="65.65.255.255" # Change this line
LAB_ENV_NAME="lab-emr-cluster"
LAB_STACK_NAME="${LAB_ENV_NAME}-stack"
LAB_KEY_NAME="${LAB_ENV_NAME}-keypair"
LAB_KEY_FILE="${LAB_KEY_NAME}.pem"
CLOUD9_PRIVATE_IP=`hostname -i`

############################################
# CREATE LAB INFRASTRUCTURE USING AWS CLI  #
############################################
aws configure set region us-east-1
export AWS_SHARED_CREDENTIALS_FILE=/home/ec2-user/.aws/credentials

CIDR_SUFFIX=
if [ "${CLIENT_IP}" = "0.0.0.0" ]; then
    CIDR_SUFFIX="/0"
else
    CIDR_SUFFIX="/32"
fi

aws ec2 create-key-pair \
    --key-name "${LAB_KEY_NAME}" \
    --query 'KeyMaterial' \
    --output text > "${LAB_KEY_FILE}"

chmod 400 "${LAB_KEY_FILE}" #change permissions

aws cloudformation deploy \
  --template-file ./template.json \
  --stack-name "lab-emr-cluster-stack" \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    Name="${LAB_ENV_NAME}" \
    InstanceType=m4.large \
    ClientIP="${CLIENT_IP}${CIDR_SUFFIX}" \
    Cloud9IP="${CLOUD9_PRIVATE_IP}/32" \
    InstanceCount=2 \
    KeyPairName="${LAB_KEY_NAME}" \
    ReleaseLabel="emr-5.32.0" \
    EbsRootVolumeSize=32

LAB_CLUSTER_ID=`aws emr list-clusters --query "Clusters[?Name=='${LAB_ENV_NAME}'].Id | [0]" --output text`
aws emr wait cluster-running --cluster-id ${LAB_CLUSTER_ID}