set -e
cd `dirname $0`
rm -rf build

mkdir build
mkdir build/api

cp -r userguide build
cp userguide/index.html build
cp -r nedoc/html/* build/api

cd build
git init
git add .
git commit -m "GitHub pages"
git remote add origin git@github.com:spirali/orco.pages.git
git push --force origin master
