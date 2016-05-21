#!/bin/bash

HOST="http://127.0.0.1"
USER="jerome"
PASS="jerome"

RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[0;33m'
MSG=${RED}
TEST=${YELLOW}
NC='\033[0m'


echo "${TEST}"
echo "---------------------"
echo "-- Test Connection --"
echo "---------------------"
echo "${NC}"
indigo exit
if [ -n PASS ]
then
    indigo init --url=${HOST} --username=${USER} --password=${PASS}
else
    echo "${MSG}Enter the password to connect to the server${NC}"
    indigo init --url=${HOST} --username=${USER}
fi
echo

echo "${TEST}"
echo "--------------"
echo "-- Test PWD --"
echo "--------------"
echo "${NC}"
indigo pwd
indigo mkdir unittest
indigo cd unittest
indigo pwd
indigo cd
indigo rm unittest
echo

echo "${TEST}"
echo "-----------------"
echo "-- Test WhoAmI --"
echo "-----------------"
echo "${NC}"
indigo whoami
echo

echo "${TEST}"
echo "-----------------------------"
echo "-- Test Collection Actions --"
echo "-----------------------------"
echo "${NC}"
indigo mkdir unittest
indigo mkdir unittest/test1
indigo cd unittest
indigo mkdir tèst
indigo mkdir test
indigo mkdir test/test
indigo cd tèst
indigo cd ..
indigo rm tèst
indigo cd test
indigo cd ../..
indigo cd
indigo rm unittest
echo


echo "${TEST}"
echo "---------------------------"
echo "-- Test Resource Actions --"
echo "---------------------------"
echo "${NC}"
echo "Test creation" > test.txt
indigo mkdir unittest
indigo cd unittest
indigo put test.txt
indigo put test.txt testé.txt
indigo put --mimetype="text/svg" test.txt test_mime.txt
indigo put --mimetype="text/svg" test.txt test_mimé.txt
indigo put --ref http://www.google.fr ref.url
indigo put --ref http://www.google.fr refé.url
indigo get test.txt test_local.txt
indigo get testé.txt
indigo get test_mime.txt
indigo get --force test_mime.txt
indigo get ref.url
indigo get refé.url
indigo cdmi test.txt
echo
indigo cdmi testé.txt
echo
indigo cdmi ref.url
echo
indigo cdmi refé.url
echo
indigo rm refé.url
indigo rm ref.url
indigo rm test_mimé.txt
indigo rm test_mime.txt
indigo rm testé.txt
indigo rm test.txt
rm test_local.txt
rm testé.txt
rm test_mime.txt
rm ref.url
rm refé.url
rm test.txt
indigo cd ..
indigo rm unittest
echo


echo "${TEST}"
echo "--------------"
echo "-- Test ACL --"
echo "--------------"
echo "${NC}"
indigo mkdir unittest
indigo mkdir unittèst
indigo chmod unittest write admins
indigo ls -a unittest
indigo chmod unittèst read admins
indigo ls -a unittèst
indigo rm unittest
indigo rm unittèst
echo

echo "${TEST}"
echo "-------------------"
echo "-- Test Metadata --"
echo "-------------------"
echo "${NC}"
indigo mkdir unittest
indigo mkdir unittèst
echo "Test creation" > test.txt
indigo put test.txt unittest/testé.txt
indigo put test.txt unittest/test.txt

indigo meta set unittest "tèst" "Vàlue"
indigo meta add unittest "tèst1" "Vàlue1"
indigo meta add unittest "tèst1" "Vàlue2"
indigo meta ls unittest
indigo meta ls unittest "tèst1"
indigo meta rm unittest "tèst1"
indigo meta rm unittest "tèst" "Vàlue"

indigo meta set unittèst "tèst" "Vàlue"
indigo meta add unittèst "tèst1" "Vàlue1"
indigo meta ls unittèst
indigo meta ls unittèst "tèst1"
indigo meta rm unittèst "tèst1"
indigo meta rm unittèst "tèst" "Vàlue"

indigo meta set unittest/test.txt "tèst" "Vàlue"
indigo meta add unittest/test.txt "tèst1" "Vàlue1"
indigo meta ls unittest/test.txt
indigo meta ls unittest/test.txt "tèst1"
indigo meta rm unittest/test.txt "tèst" "Vàlue"
indigo meta rm unittest/test.txt "tèst1"

indigo meta set unittest/testé.txt "tèst" "Vàlue"
indigo meta add unittest/testé.txt "tèst1" "Vàlue1"
indigo meta add unittest/testé.txt "tèst1" "Vàlue2"
indigo meta ls unittest/testé.txt
indigo meta ls unittest/testé.txt "tèst1"
indigo meta rm unittest/testé.txt "tèst" "Vàlue"
indigo meta rm unittest/testé.txt "tèst1"

indigo cd unittest
indigo meta set . "tèst" "Vàlue"
indigo meta set . "tèst1" "Vàlue1"
indigo meta ls .
indigo meta ls . "tèst1"
echo


echo "${TEST}"
echo "---------------"
echo "-- Test User --"
echo "---------------"
echo "${NC}"
indigo admin lu
indigo admin mkuser jerome1
indigo admin mkuser jérôme
indigo admin mkuser tést
indigo admin rmuser
indigo admin lu
indigo admin lu jérôme
indigo admin moduser jérôme email new_email
indigo admin moduser jérôme administrator False 
indigo admin moduser jérôme active False 
indigo admin moduser jérôme password NewPassword
indigo admin lu jérôme

indigo admin lg
indigo admin mkgroup jérômeG
indigo admin lg jérômeG
indigo admin atg jérômeG jérôme  tést
indigo admin rtg jérômeG jérôme  tést

indigo admin rmuser jérôme
indigo admin rmuser jerome1
indigo admin rmuser tést
indigo admin rmgroup jérômeG

exit

