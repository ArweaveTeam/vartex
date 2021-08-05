{ pkgs ? import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/60b891a438e52919def82ee2c7eb3835d68db9a3.tar.gz") {} }:

let cassandra4x = (pkgs.cassandra.overrideAttrs(o: {
      version = "4.0.0";
      src = pkgs.fetchurl {
        sha256 = "1wf2i91wgq89262w539hkvdx4dbzh1ln3zi69cnhmi96f7d7pw9g";
        url = "https://apache.mirror.digionline.de/cassandra/4.0.0/apache-cassandra-4.0.0-bin.tar.gz";
      };
      preInstall = "touch javadoc";
    }));
    yarn_latest = pkgs.yarn.override { nodejs = pkgs.nodejs_latest; };
in pkgs.mkShell {
  buildInputs = with pkgs; [
    cassandra4x
    openssl
    yarn_latest
    nodejs_latest
  ];
}
