#!/bin/bash

if [ "$TRAVIS_BRANCH" == "main" ]; then

    cd $TRAVIS_BUILD_DIR

    # This is not a pull request in travis. Configure kubectl, eksctl
    if [ -z "$TRAVIS_PULL_REQUEST" ] || [ "$TRAVIS_PULL_REQUEST" == "false" ]; then

        aws eks update-kubeconfig --name koi --region us-west-2
        kubectl apply -f k8s/gateway.yaml -n koi
        kubectl rollout restart deployment gateway -n koi

    fi

fi