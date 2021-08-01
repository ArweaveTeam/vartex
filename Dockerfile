FROM node:16.6-buster

ARG CASSANDRA_CONTACT_POINTS

ENV CASSANDRA_CONTACT_POINTS ${CASSANDRA_CONTACT_POINTS}

RUN apt-get update && apt-get install -y \
  curl \
  sudo \
  xz-utils \
  systemd \
  unzip

RUN mkdir -m 0755 /nix && chown 1000:1000 /nix
RUN groupadd -r nixbld
RUN for n in $(seq 1 10); do useradd -c "Nix build user $n" \
      -d /var/empty -g nixbld -G nixbld -M -N -r -s "$(which nologin)" \
      nixbld$n; done

RUN su node
ENV HOME=/home/node
ENV PATH=$PATH:$HOME/.nix-profile/bin
ENV NIX_PATH=$HOME/.nix-defexpr/channels
RUN curl -L https://nixos.org/nix/install | sh
RUN . $HOME/.nix-profile/etc/profile.d/nix.sh

WORKDIR $HOME/gateway-cassandra

RUN nix-channel --add https://nixos.org/channels/nixpkgs-unstable nixpkgs
RUN nix-channel --update
RUN nix-env -f '<nixpkgs>' -iA nodejs yarn
COPY . .
RUN rm -f ./.env* # mixing configurations is too confusing
RUN yarn install

EXPOSE 3000

CMD ["yarn", "start:docker"]
