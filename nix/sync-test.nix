{ pkgs ? import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/60b891a438e52919def82ee2c7eb3835d68db9a3.tar.gz") {
  overlays = [ (import ./overlay.nix) ];
} }:


pkgs.mkShell {
  buildInputs = with pkgs; [
    git
    cassandra
    openssl
    yarn
    nodejs
  ];

  shellHook = ''
    ${pkgs.cassandraShellHook}
    yarn install
    yarn test:sync || exit 1
  '';

}
