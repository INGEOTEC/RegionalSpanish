for cc in DO.txt CO.txt BR.txt PY.txt EC.txt ES.txt CA.txt GB.txt GQ.txt CL.txt GT.txt UY.txt FR.txt SV.txt PA.txt VE.txt MX.txt NI.txt HN.txt US.txt CR.txt BO.txt PR.txt CU.txt PE.txt AR.txt
do
	srun -N1 -xgeoint0 python create-model.py $cc	
done
