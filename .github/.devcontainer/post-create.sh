# post-create.sh
# This script is executed after the container is created.

# Create custom bash prompt
echo "" >> ~/.bashrc
echo "# Custom bash prompt" >> ~/.bashrc
echo "PS1='\e[0;32m\u\e[0m → \e[0;34m\W\e[0m \$ '" >> ~/.bashrc
echo "" >> ~/.bashrc