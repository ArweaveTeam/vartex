final: prev: {
  cassandra = (prev.cassandra.overrideAttrs(o: {
    version = "4.0.0";
    src = prev.fetchurl {
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

  cassandraShellHook = ''
    export GIT_SSL_CAINFO=/etc/ssl/certs/ca-certificates.crt
    export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
    export CASSANDRA_LOG_DIR="$(pwd)/logs"
    export JVM_OPTS="-Dcassandra.storagedir=/etc/defaults/cassandra"
    (&>/dev/null cassandra -f &)
  '';

  nodejs = prev.nodejs_latest;

  yarn = prev.yarn.override { nodejs = final.nodejs; };

}
