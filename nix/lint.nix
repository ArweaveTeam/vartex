{ pkgs ? import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/60b891a438e52919def82ee2c7eb3835d68db9a3.tar.gz") {} }:

let yarn_latest = pkgs.yarn.override { nodejs = pkgs.nodejs_latest; };

in pkgs.mkShell {
  buildInputs = with pkgs; [
    git
    openssl
    yarn_latest
    nodejs_latest
  ];

  shellHook = ''
    export GIT_SSL_CAINFO=/etc/ssl/certs/ca-certificates.crt
    export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
    yarn install
    yarn lint || exit 1
  '';

}
