#!/bin/sh

rm -fr sandbox
git clone https://github.com/sonata-project/sandbox.git sandbox
cd sandbox
git checkout master

cp ../app/.env ./.env
mkdir -p ./var/db

composer update

#rm ./src/AppBundle/Resources/config/doctrine/Classification.*
#rm ./src/AppBundle/Resources/config/doctrine/Page.*
#rm ./src/AppBundle/Resources/config/serializer/Entity.Page.*
# rm ./src/AppBundle/Entity/Classification/*

#composer require --dev symfony/maker-bundle symfony/web-server-bundle
#composer require sonata-project/admin-bundle:3.68.0 league/csv scheb/yahoo-finance-api

./bin/console --env=dev doctrine:database:create
./bin/console --env=dev doctrine:schema:create
./bin/console --env=dev fos:user:create --super-admin admin admin@domain.com admin

./bin/console --env=dev cache:warmup
symfony serve --no-tls
