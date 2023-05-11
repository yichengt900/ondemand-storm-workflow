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
cp -v /contrib/Soroosh.Mani/scripts/schism.sbatch ~
cp -v /contrib/Soroosh.Mani/scripts/combine_gr3.exp ~

cp -L -r /contrib/Soroosh.Mani/pkgs/schism .

echo "export PATH=\$HOME/schism/bin/:\$PATH" >> ~/.bash_profile
echo "export PATH=\$HOME/schism/bin/:\$PATH" >> ~/.bashrc

cp -v /contrib/Soroosh.Mani/pkgs/odssm-prefect.tar.gz .
mkdir odssm-prefect
pushd odssm-prefect
tar -xf ../odssm-prefect.tar.gz
rm -rf ../odssm-prefect.tar.gz
bin/conda-unpack
popd

# No static files is needed for run!
date > ~/_initialized_

# This is executed only for head (ALLNODES not specified at the top)
source odssm-prefect/bin/activate
prefect cloud login --key `cat /contrib/Soroosh.Mani/secrets/prefect.key` --workspace sorooshmaninoaagov/ondemand-workflow && prefect agent start -q test-pw-solve
