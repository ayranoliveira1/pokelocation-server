# Deploy na VPS

Ubuntu 24.04 com Docker e o nginx do sistema nas portas 80/443, que pode ter outros sites e containers. Nada aqui
mexe neles: sem Caddy, sem portas publicadas além do servidor em `127.0.0.1:8095`, sem `daemon.json` nem restart do
Docker, e um site novo no nginx em vez de editar os existentes. Domínio
`pokelocation.waveplay.xyz` com registro A para o IP da VPS na Cloudflare em **DNS only**.

## 1. Código

O repo é privado. Na VPS, uma chave só para ele (não a `id_ed25519`, que pode ser de outro projeto):

```sh
ssh-keygen -t ed25519 -f ~/.ssh/pokelocation_deploy -N '' -C pokelocation-deploy
printf 'Host github-pokelocation\n  HostName github.com\n  User git\n  IdentityFile ~/.ssh/pokelocation_deploy\n  IdentitiesOnly yes\n' >> ~/.ssh/config
```

Cadastre a `.pub` em Settings → Deploy keys do repo, **sem** permissão de escrita. Depois:

```sh
sudo install -d -o "$USER" -g "$USER" /opt/pokelocation-server /opt/pokelocation-static
git clone -b main github-pokelocation:ayranoliveira1/pokelocation-server.git /opt/pokelocation-server
```

`/opt` em vez da home: o nginx (`www-data`) lê `deploy/site/privacy.html` e a home é `750`.

## 2. Segredos

Do Mac, sem abrir o arquivo:

```sh
ssh <usuário>@<ip> 'mkdir -m 700 -p ~/pokelocation-secrets'
scp ~/pokelocation-secrets/fcm-service-account.json <usuário>@<ip>:pokelocation-secrets/
```

Na VPS, o servidor roda como o usuário 65532 (distroless nonroot) e precisa ler o JSON:

```sh
sudo chown 65532 ~/pokelocation-secrets/fcm-service-account.json
sudo chmod 400 ~/pokelocation-secrets/fcm-service-account.json
```

`/opt/pokelocation-server/.env` (`chmod 600`):

```sh
POSTGRES_PASSWORD=<openssl rand -base64 32>
AUTH_SIGNING_KEY=<openssl rand -base64 32>
COMPOSE_FILE=docker-compose.yml:deploy/compose.prod.yml
PUBLIC_URL=https://pokelocation.waveplay.xyz
FCM_CREDENTIALS_FILE=/home/<usuário>/pokelocation-secrets/fcm-service-account.json
GOMEMLIMIT=2GiB
GOOGLE_CLIENT_ID=
GOOGLE_CLIENT_SECRET=
BACKUP_PAR_URL=
```

Os `.so.gz` da libsignal são reserva do download do GitHub: copie `tools/libsignal/out/0.86.5` do repo do app para
`/opt/pokelocation-static/libsignal-0.86.5/`.

## 3. nginx e TLS

```sh
sudo cp /opt/pokelocation-server/deploy/nginx.conf /etc/nginx/sites-available/pokelocation.waveplay.xyz
sudo ln -s /etc/nginx/sites-available/pokelocation.waveplay.xyz /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
sudo certbot --nginx -d pokelocation.waveplay.xyz --redirect
```

O certbot só edita o arquivo deste site e renova sozinho pelo timer que já existe.

## 4. Subir e conferir

```sh
cd /opt/pokelocation-server
docker compose up -d --build
curl -fsS https://pokelocation.waveplay.xyz/healthz
curl -fsSI https://pokelocation.waveplay.xyz/privacy
docker compose logs server | grep -v '"level":"INFO"'   # vazio = sem aviso nem erro
```

Atualizar: `git pull && docker compose up -d --build`. As migrações rodam na subida.

## 5. Backup diário

No console: bucket privado no Object Storage, com uma regra de lifecycle que apaga objetos com mais de 30 dias. A
regra só roda com a política
`Allow service objectstorage-sa-saopaulo-1 to manage object-family in tenancy`. Crie uma Pre-Authenticated Request
no bucket, do tipo **Permit object writes**, e ponha a URL (termina em `/o/`) em `BACKUP_PAR_URL` no `.env`, sem
aspas. A URL é segredo: quem a tem grava no bucket. A PAR exige data de expiração: escolha uma longa e anote a data
para renovar, porque depois dela o backup só falha no `journalctl`.

```sh
deploy/backup.sh && echo ok          # teste: o objeto pokelocation-<data>.sql.gz aparece no bucket
sed "s/^User=.*/User=$USER/" deploy/pokelocation-backup.service | sudo tee /etc/systemd/system/pokelocation-backup.service >/dev/null
sudo cp deploy/pokelocation-backup.timer /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now pokelocation-backup.timer
systemctl list-timers pokelocation-backup.timer
journalctl -u pokelocation-backup.service   # falhas aparecem aqui
```

Roda entre 03:30 e 03:45 de Brasília. Com `BACKUP_PAR_URL` vazio, sai sem fazer nada.

## Restore

Baixe o `.sql.gz` pelo console (a PAR é só de escrita) e copie para a VM. O dump apaga e recria as tabelas:

```sh
cd /opt/pokelocation-server
docker compose stop server
gunzip -c pokelocation-<data>.sql.gz | docker compose exec -T postgres psql -U pokelocation -d pokelocation -v ON_ERROR_STOP=1 -1
docker compose start server
```
