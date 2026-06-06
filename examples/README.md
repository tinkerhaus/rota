# Rota examples

Start a local broker first:

```bash
go run ./cmd/rota serve --grpc 127.0.0.1:7100 --metrics 127.0.0.1:7101
```

Python examples use the SDK from this checkout:

```bash
PYTHONPATH=sdk/python python examples/python/fair_email_worker.py
PYTHONPATH=sdk/python python examples/python/payment_workflow.py
```

TypeScript examples import the built SDK from `sdk/typescript/dist`:

```bash
cd sdk/typescript
npm install
npm run build
cd ../..
./sdk/typescript/node_modules/.bin/tsx examples/typescript/background-worker.ts
./sdk/typescript/node_modules/.bin/tsx examples/typescript/payment-workflow.ts
```

Each example accepts `--addr` or `ROTA_ADDR`; the default is `127.0.0.1:7100`.
In an application outside this repository, replace the TypeScript import path
with `rota` after installing the package.
