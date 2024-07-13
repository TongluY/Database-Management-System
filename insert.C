#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{

	//cout << "Doing QU_Insert" << endl;

	Status status;
	int attrCntValue;
	AttrDesc* attrs;


	if((status = attrCat->getRelInfo(relation, attrCntValue, attrs)) != OK) return status;
	
	if(attrCntValue != attrCnt) {
		delete[] attrs;
		return UNIXERR;}
	
	int totalAttrLen = 0;
	for (int i = 0; i < attrCnt; i++){
		totalAttrLen += attrs[i].attrLen;
		if ((status = attrCat->getInfo(relation, attrList[i].attrName, attrs[i])) != OK) return status;	//?
	}

	char* insertData = new char[totalAttrLen];
	Record rec;
	rec.data = (void*) insertData;
	rec.length = totalAttrLen;

	for (int i = 0; i < attrCnt; i++) {
        AttrDesc &attr = attrs[i]; 
        const attrInfo &info = attrList[i]; 

        switch (attr.attrType) {
            case INTEGER: {
                int valInt = atoi((char*) info.attrValue);
                memcpy(insertData + attr.attrOffset, &valInt, attr.attrLen);
                break;
            }
            case STRING:
                memcpy(insertData + attr.attrOffset, info.attrValue, attr.attrLen);
                break;
            case FLOAT: {
                float valFloat = atof((char*) info.attrValue);
                memcpy(insertData + attr.attrOffset, &valFloat, attr.attrLen);
                break;
            }
            default:
                delete[] attrs;
                delete[] insertData;
                return UNIXERR;
        }
    }
	InsertFileScan* scanIF = new InsertFileScan(relation, status);
    if (status != OK) {
        delete[] attrs;
        delete[] insertData;
        delete scanIF; 
        return status;
    }

    RID rid;
    if ((status = scanIF->insertRecord(rec, rid)) != OK) {
        delete[] attrs;
        delete[] insertData;
        delete scanIF; 
        return status;
    }

    // Clean up
    delete[] attrs;
    delete[] insertData;
    delete scanIF; 
    return status;
}

