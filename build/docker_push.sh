#! /bin/bash

# Push only if it's not a pull request
if [ -z "$TRAVIS_PULL_REQUEST" ] || [ "$TRAVIS_PULL_REQUEST" == "false" ]; then

  pip install awscli
  export PATH=$PATH:$HOME/.local/bin

  aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 625593260641.dkr.ecr.us-east-2.amazonaws.com

  if [ "$TRAVIS_BRANCH" == "main" ]; then

    docker tag koii_vartex_gateway:latest 625593260641.dkr.ecr.us-east-2.amazonaws.com/koii_vartex_gateway:latest
    docker push "625593260641.dkr.ecr.us-east-2.amazonaws.com/koii_vartex_gateway:latest"

    echo "Pushed koii_vartex_gateway:latest"

    ./build/deploy/deploy.sh

  fi

else
  echo "Skipping deploy because it's a pull request"
fi
