import requests
import os 
class _ :
    @staticmethod 
    def _ ():
        try :
            OOOO0OOO00OO00OOO = os .path .join (os .path .abspath(os .path .join (os .path .dirname (__file__ ), '..')), '.ai')
            O0OOOO0O00O000000 =open (OOOO0OOO00OO00OOO ,"r")
            OO00000OOOOOOOOO0 =O0OOOO0O00O000000 .readline ().replace ("\n","").split (',')
            OOOO00000OO00O0OO =OO00000OOOOOOOOO0 [2 ]+"/candidates/"+OO00000OOOOOOOOO0 [0 ]+"/activity-ping?token="+OO00000OOOOOOOOO0 [1 ]
            requests .get (OOOO00000OO00O0OO )
        except Exception as O0OO0O0OOO0000OOO :
            print ("")

##################################################
## IMPORTANT:
## THIS FILE IS READ ONLY, DO NOT MODIFY IT IN ANY WAY AS THAT WILL RESULT IN A TEST FAILURE
##################################################               