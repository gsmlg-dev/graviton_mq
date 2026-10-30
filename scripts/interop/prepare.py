"""Prepare hash-pinned, test-only independent peers in an explicit directory."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile
import urllib.request
import venv

HERE = Path(__file__).resolve().parent


def checked(command, **options):
    subprocess.run([str(arg) for arg in command], check=True, timeout=600, **options)


def download(peer, directory):
    path = directory / peer["url"].rsplit("/", 1)[1]
    algorithm = "sha512" if "sha512" in peer else "sha256"
    if not path.exists():
        temporary = path.with_suffix(".download")
        try:
            with urllib.request.urlopen(peer["url"], timeout=60) as response, temporary.open("wb") as output:
                copied = 0
                while chunk := response.read(1_048_576):
                    copied += len(chunk)
                    if copied > 268_435_456:
                        raise ValueError("peer archive exceeds download limit")
                    output.write(chunk)
            temporary.rename(path)
        finally:
            temporary.unlink(missing_ok=True)
    digest = hashlib.new(algorithm)
    with path.open("rb") as source:
        while chunk := source.read(1_048_576):
            digest.update(chunk)
    if digest.hexdigest() != peer[algorithm]:
        raise ValueError(f"peer checksum mismatch: {path}")
    return path


def extract(archive, directory):
    with tarfile.open(archive) as source:
        source.extractall(directory, filter="data")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--provided", action="store_true", help="validate and reuse explicit peer environment paths")
    args = parser.parse_args()
    directory = args.directory.resolve()
    directory.mkdir(parents=True, exist_ok=True)
    peers = json.loads((HERE / "peers.json").read_text())
    environment = {}
    if args.provided:
        for key in ["GRAVITON_MOSQUITTO_BIN", "GRAVITON_PAHO_PYTHON", "GRAVITON_PROTON_PYTHON", "GRAVITON_ARTEMIS_HOME", "JAVA_HOME"]:
            environment[key] = os.environ[key]
    else:
        mosquitto = download(peers["mosquitto"], directory)
        artemis = download(peers["artemis"], directory)
        source = directory / "mosquitto-2.1.2"
        if not source.exists():
            extract(mosquitto, directory)
        if not (directory / "apache-artemis-2.41.0").exists():
            extract(artemis, directory)
        prefix = directory / "mosquitto-install"
        build = directory / "mosquitto-build"
        checked(["cmake", "-S", source, "-B", build, "-DCMAKE_BUILD_TYPE=Release", f"-DCMAKE_INSTALL_PREFIX={prefix}",
                 "-DWITH_DOCS=OFF", "-DWITH_TESTS=OFF", "-DWITH_APPS=ON", "-DWITH_PLUGINS=OFF",
                 "-DWITH_WEBSOCKETS=OFF", "-DWITH_CTRL_SHELL=OFF", "-DWITH_TLS=ON", "-DWITH_LTO=OFF"])
        checked(["cmake", "--build", build, "--parallel", "2"])
        checked(["cmake", "--install", build])
        # Tests intentionally locate CLI peers next to the broker executable.
        broker = prefix / "sbin/mosquitto"
        for client in ["mosquitto_pub", "mosquitto_sub", "mosquitto_passwd"]:
            link = broker.parent / client
            if not link.exists():
                link.symlink_to(prefix / "bin" / client)
        python = directory / "python/bin/python"
        venv.EnvBuilder(with_pip=True).create(directory / "python")
        checked([python, "-m", "pip", "install", "--require-hashes", "-r", HERE / "build-requirements.txt"])
        # Proton's source build needs cffi already importable when setup runs.
        # Otherwise its setup can produce a pure wheel without cproton_ffi.
        checked([python, "-m", "pip", "install", "--require-hashes", "--no-build-isolation", "--no-binary", "cffi",
                 "-r", HERE / "ffi-requirements.txt"])
        checked([python, "-m", "pip", "install", "--require-hashes", "--no-build-isolation", "--no-deps", "--no-cache-dir", "--force-reinstall", "--no-binary", "python-qpid-proton",
                 "-r", HERE / "requirements.txt"])
        environment = {
            "GRAVITON_MOSQUITTO_BIN": str(broker), "GRAVITON_PAHO_PYTHON": str(python),
            "GRAVITON_PROTON_PYTHON": str(python), "GRAVITON_ARTEMIS_HOME": str(directory / "apache-artemis-2.41.0"),
            "JAVA_HOME": os.environ["JAVA_HOME"],
            "LD_LIBRARY_PATH": os.pathsep.join([str(prefix / "lib"), str(prefix / "lib64"), os.environ.get("LD_LIBRARY_PATH", "")])
        }
    env = dict(os.environ, **environment)
    for name in ["mosquitto_pub", "mosquitto_sub", "mosquitto_passwd"]:
        executable = Path(environment["GRAVITON_MOSQUITTO_BIN"]).parent / name
        if not executable.is_file() or not os.access(executable, os.X_OK):
            raise ValueError(f"missing interoperability executable: {executable}")
    checked(["openssl", "version"], env=env)
    version = subprocess.check_output([environment["GRAVITON_MOSQUITTO_BIN"], "-h"], env=env, text=True, timeout=10)
    if "mosquitto version 2.1.2" not in version:
        raise ValueError("unexpected Mosquitto version")
    checked([environment["GRAVITON_PAHO_PYTHON"], "-c", "from importlib.metadata import version; assert version('paho-mqtt') == '2.1.0'"], env=env)
    checked([environment["GRAVITON_PROTON_PYTHON"], "-c", "from importlib.metadata import version; from proton import SSL; assert version('python-qpid-proton') == '0.40.0'; assert SSL.present()"], env=env)
    checked([Path(environment["JAVA_HOME"]) / "bin/java", "-version"], env=env)
    (directory / "environment.json").write_text(json.dumps(environment, indent=2) + "\n")
    print(f"Pinned peers prepared: {directory}", flush=True)


if __name__ == "__main__":
    main()
