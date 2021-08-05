{ pkgs ? import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/60b891a438e52919def82ee2c7eb3835d68db9a3.tar.gz") {} }:

let cassandra4x = (pkgs.cassandra.overrideAttrs(o: {
      version = "4.0.0";
      src = pkgs.fetchurl {
        sha256 = "1wf2i91wgq89262w539hkvdx4dbzh1ln3zi69cnhmi96f7d7pw9g";
        url = "https://apache.mirror.digionline.de/cassandra/4.0.0/apache-cassandra-4.0.0-bin.tar.gz";
      };
      preInstall = "touch javadoc";
      postInstall = ''
        echo "\n" 'data_file_directories: "/var/lib/cassandra/data"' >> $out/conf/cassandra.yml
        sed -i -e 's|cassandra_storagedir=.*|cassandra_storagedir="/var/lib/cassandra/data"|g' \
          $out/bin/cassandra.in.sh
      '';
    }));
    yarn_latest = pkgs.yarn.override { nodejs = pkgs.nodejs_latest; };

in pkgs.mkShell {
  buildInputs = with pkgs; [
    git
    cassandra4x
    openssl
    yarn_latest
    nodejs_latest
  ];

  shellHook = ''
    export GIT_SSL_CAINFO=/etc/ssl/certs/ca-certificates.crt
    export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
    export CASSANDRA_LOG_DIR="$(pwd)/logs"
    export JVM_OPTS="-Dcassandra.storagedir=/etc/defaults/cassandra"
    (&>/dev/null cassandra -f &)
    yarn install
    yarn test
  '';

}
