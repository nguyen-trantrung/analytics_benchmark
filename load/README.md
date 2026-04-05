# Run with `proxychains4`

## Install

```bash
brew install proxychains-ng
```

## Run

```bash
ssh -D 1080 -o 'ServerAliveInterval 60' -N root@192.168.100.1
proxychains4 -f ./load/proxychains4.conf python3 load/load_data_tidb.py
```
