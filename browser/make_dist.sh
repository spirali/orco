
set -e

cd `dirname $0`

TARGET=../orco/static

npm run build

rm -rf dist
mkdir dist

cp build/*.js build/*.html build/*.json dist/
cp -r build/static/js dist/js
cp -r build/static/css dist/css

gzip dist/js/*
gzip dist/css/*
gzip dist/*.js dist/*.html dist/*.json

mkdir -p ${TARGET}
rm -rf ${TARGET}
cp -r dist ${TARGET}
