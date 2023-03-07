DISOWN

# We want the disowned script only on head node
if [ "$(hostname | grep -o mgmt)"  != "mgmt" ]; then
    exit
fi

export PATH=$PATH:/usr/local/bin

sudo yum update -y && sudo yum upgrade -y

# TODO: Use lustre instead of home
sudo yum install -y tmux
cp -v /contrib/Soroosh.Mani/configs/.vimrc ~
cp -v /contrib/Soroosh.Mani/configs/.tmux.conf ~

cd ~
cp -v /contrib/Soroosh.Mani/scripts/hurricane_mesh.py ~
cp -v /contrib/Soroosh.Mani/scripts/mesh.sbatch ~

cp -v /contrib/Soroosh.Mani/pkgs/odssm-mesh.tar.gz .
mkdir odssm-mesh
pushd odssm-mesh
tar -xf ../odssm-mesh.tar.gz
rm -rf ../odssm-mesh.tar.gz
bin/conda-unpack
popd

cp -v /contrib/Soroosh.Mani/pkgs/odssm-prefect.tar.gz .
mkdir odssm-prefect
pushd odssm-prefect
tar -xf ../odssm-prefect.tar.gz
rm -rf ../odssm-prefect.tar.gz
bin/conda-unpack
popd

aws s3 sync s3://noaa-nos-none-ca-hsofs-c/Soroosh.Mani/dem /lustre/dem
aws s3 sync s3://noaa-nos-none-ca-hsofs-c/Soroosh.Mani/shape /lustre/shape
aws s3 sync s3://noaa-nos-none-ca-hsofs-c/Soroosh.Mani/grid /lustre/grid
date > ~/_initialized_

# This is executed only for head (ALLNODES not specified at the top)
source odssm-prefect/bin/activate
prefect agent local start --key  `cat /contrib/Soroosh.Mani/secrets/prefect.key` --label tacc-odssm-rdhpcs-mesh-cluster --name tacc-odssm-agent-rdhpcs-mesh-cluster --log-level INFO
