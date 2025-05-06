#! /usr/bin/bash

if ! command -v rustup &> /dev/null
then
    echo "Rust doesn't exist."
    echo "Installing Rust."
    curl https://sh.rustup.rs -sSf | sh -s -- -y
    . "$HOME/.cargo/env"
fi
echo "Compiling program."
cargo build -r
cp target/release/hashgen .
echo "Compiled hashgen"
./hashgen -h