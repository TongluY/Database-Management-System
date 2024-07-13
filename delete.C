#include "catalog.h"
#include "query.h"

/*
 * Deletes records from a specified relation.
 *
 * Returns:
 *  OK on success
 *  an error code otherwise
 */
const Status QU_Delete(const string &relation, 
                       const string &attrName, 
                       const Operator op,
                       const Datatype type, 
                       const char *attrValue) {
    // cout << "Doing QU_Delete" << endl;

    // Check if the relation name is empty
    if (relation.empty()) {
        return BADCATPARM;
    }

    Status status;
    RID rid;
    HeapFileScan *scanner = new HeapFileScan(relation, status);
    if (status != OK) {
        return status;
    }

    // Check if attribute name is empty
    if (attrName.empty()) {
        scanner->startScan(0, 0, STRING, NULL, EQ);
    } else {
        AttrDesc attrD;
        status = attrCat->getInfo(relation, attrName, attrD);
        if (status != OK) {
            delete scanner;
            return status;
        }

        int valInt;
        float valFloat;
        switch (type) {
            case INTEGER:
                valInt = atoi(attrValue);
                status = scanner->startScan(attrD.attrOffset, attrD.attrLen, type, (char *)&valInt, op);
                break;

            case FLOAT:
                valFloat = atof(attrValue);
                status = scanner->startScan(attrD.attrOffset, attrD.attrLen, type, (char *)&valFloat, op);
                break;

            default:
                status = scanner->startScan(attrD.attrOffset, attrD.attrLen, type, attrValue, op);
                break;
        }
    }

    if (status != OK) {
        delete scanner;
        return status;
    }

    while ((status = scanner->scanNext(rid)) == OK) {
        status = scanner->deleteRecord();
        if (status != OK) {
            delete scanner;
            return status;
        }
    }

    if (status != FILEEOF) {
        delete scanner;
        return status;
    }

    scanner->endScan();
    delete scanner;

    return OK;
}
