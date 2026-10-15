# Deploy na VM (Oracle Cloud Always Free)

VM.Standard.A1.Flex (ARM64, 2 OCPU, 12 GB), Ubuntu 24.04, São Paulo. Domínio `pokelocation.waveplay.xyz` com
registro A para o IP público da VM na Cloudflare em **DNS only** (o Caddy tira o certificado do Let's Encrypt sozinho).

## 1. Rede

No console da Oracle, na Security List da subnet da VM: ingress TCP 80 e 443 de `0.0.0.0/0`.

A imagem Ubuntu da Oracle traz um `REJECT` no fim da chain INPUT. Libere 80/443 **antes** de instalar o Docker, para
o `save` não gravar as regras do Docker:

```sh
sudo iptables -I INPUT -p tcp -m multiport --dports 80,443 -m conntrack --ctstate NEW -j ACCEPT
sudo netfilter-persistent save
```

## 2. Docker e código

```sh
curl -fsSL https://get.docker.com | sudo sh
echo '{"log-driver": "local"}' | sudo tee /etc/docker/daemon.json && sudo systemctl restart docker
sudo usermod -aG docker ubuntu && exit   # entre de novo para valer o grupo
git clone -b development git@github.com:ayranoliveira1/pokelocation-server.git ~/pokelocation-server
```

O repo é privado: na VM, `ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519 -N ''` e cadastre a `.pub` em Settings → Deploy
keys do repo, **sem** permissão de escrita, antes do `git clone`.

O driver `local` rotaciona os logs dos containers (o `json-file` padrão cresce sem limite).

## 3. Segredos

Do Mac, sem abrir o arquivo:

```sh
ssh ubuntu@<ip> 'mkdir -m 700 -p ~/pokelocation-secrets'
scp ~/pokelocation-secrets/fcm-service-account.json ubuntu@<ip>:pokelocation-secrets/
```

Na VM, o servidor roda como o usuário 65532 (distroless nonroot) e precisa ler o JSON:

```sh
sudo chown 65532 ~/pokelocation-secrets/fcm-service-account.json
sudo chmod 400 ~/pokelocation-secrets/fcm-service-account.json
```

`~/pokelocation-server/.env` (`chmod 600`), a partir do `.env.example`:

```sh
POSTGRES_PASSWORD=<openssl rand -base64 32>
AUTH_SIGNING_KEY=<openssl rand -base64 32>
COMPOSE_FILE=docker-compose.yml:deploy/compose.prod.yml
PUBLIC_URL=https://pokelocation.waveplay.xyz
SITE_ADDRESS=pokelocation.waveplay.xyz
FCM_CREDENTIALS_FILE=/home/ubuntu/pokelocation-secrets/fcm-service-account.json
GOMEMLIMIT=6GiB
BACKUP_PAR_URL=
```

`GOMEMLIMIT=6GiB`: metade da VM, com o resto para Postgres, Redis, Caddy e o cache de disco. Os `.so.gz` da
libsignal são só reserva do download do GitHub; para servi-los, copie `tools/libsignal/out/0.86.5` do repo do app
para a VM e aponte `LIBSIGNAL_DIR` para lá.

## 4. Subir e conferir

```sh
cd ~/pokelocation-server
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
sudo cp deploy/pokelocation-backup.service deploy/pokelocation-backup.timer /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now pokelocation-backup.timer
systemctl list-timers pokelocation-backup.timer
journalctl -u pokelocation-backup.service   # falhas aparecem aqui
```

Roda entre 03:30 e 03:45 de Brasília. Com `BACKUP_PAR_URL` vazio, sai sem fazer nada.

## Restore

Baixe o `.sql.gz` pelo console (a PAR é só de escrita) e copie para a VM. O dump apaga e recria as tabelas:

```sh
cd ~/pokelocation-server
docker compose stop server
gunzip -c pokelocation-<data>.sql.gz | docker compose exec -T postgres psql -U pokelocation -d pokelocation -v ON_ERROR_STOP=1 -1
docker compose start server
```
