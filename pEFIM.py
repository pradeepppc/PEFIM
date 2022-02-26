from functools import cmp_to_key
from Transaction import Transaction

class pEFIM():
    highUtilityItemsets = []
    candidateCount = 0
    utilityBinArrayLU = {}
    utilityBinArraySU = {}

    # a temporary buffer
    temp = []
    for i in range(5000):
        temp.append(0)
    
    def __init__(self, minUtility, itemsToExplore, itemsToKeep, transactions, newNamesToOldNames, oldNamesToNewNames):
        self.minUtil = minUtility
        self.itemsToExplore = itemsToExplore
        self.itemsToKeep = itemsToKeep
        self.transactions = transactions
        self.newNamesToOldNames = newNamesToOldNames
        self.oldNamesToNewNames  = oldNamesToNewNames
    
    def runAlgo(self):
        # now we will sort the transactions according to proposed total order on transaction
        self.sortDatabase(self.transactions)
        self.backtrackingEFIM(self.transactions, self.itemsToKeep, self.itemsToExplore, 0)
        return (1, self.highUtilityItemsets)
    
    def backtrackingEFIM(self, transactionsOfP, itemsToKeep, itemsToExplore, prefixLength):
        self.candidateCount += len(itemsToExplore)
        for idx, e in enumerate(itemsToExplore):
            # caluclate the transactions containing p U {e}
            # at the same time project transactions to keep what appears after e
            transactionsPe = []
            # variable to caluclate the utility of Pe
            utilityPe = 0
            # merging transactions
            previousTransaction = transactionsOfP[0]
            consecutiveMergeCount = 0
            for transaction in transactionsOfP:
                items = transaction.getItems()
                if e in items:
                    # if e was found in the transaction
                    positionE = items.index(e)
                    if transaction.getLastPosition() == positionE:
                        utilityPe += transaction.getUtilities()[positionE] + transaction.prefixUtility
                    else:
                        projectedTransaction = transaction.projectTransaction(positionE)
                        utilityPe += projectedTransaction.prefixUtility
                        if previousTransaction == transactionsOfP[0]:
                            # if it is the first transactoin
                            previousTransaction = projectedTransaction
                        elif self.is_equal(projectedTransaction, previousTransaction):
                            if consecutiveMergeCount == 0:
                                # if the first consecutive merge
                                items = previousTransaction.items[previousTransaction.offset:]
                                utilities = previousTransaction.utilities[previousTransaction.offset:]
                                itemsCount = len(items)
                                positionPrevious = 0
                                positionProjection = projectedTransaction.offset
                                while positionPrevious < itemsCount:
                                    utilities[positionPrevious] += projectedTransaction.utilities[positionProjection]
                                    positionPrevious += 1
                                    positionProjection += 1
                                previousTransaction.prefixUtility += projectedTransaction.prefixUtility
                                sumUtilities = previousTransaction.prefixUtility
                                previousTransaction = Transaction(items, utilities, previousTransaction.transactionUtility + projectedTransaction.transactionUtility)
                                previousTransaction.prefixUtility = sumUtilities
                            else:
                                positionPrevious = 0
                                positionProjected = projectedTransaction.offset
                                itemsCount = len(previousTransaction.items)
                                while positionPrevious < itemsCount:
                                    previousTransaction.utilities[positionPrevious] += projectedTransaction.utilities[
                                        positionProjected]
                                    positionPrevious += 1
                                    positionProjected += 1
                                previousTransaction.transactionUtility += projectedTransaction.transactionUtility
                                previousTransaction.prefixUtility += projectedTransaction.prefixUtility
                            consecutiveMergeCount += 1
                        else:
                            transactionsPe.append(previousTransaction)
                            previousTransaction = projectedTransaction
                            consecutiveMergeCount = 0
                    transaction.offset = positionE
            if previousTransaction != transactionsOfP[0]:
                transactionsPe.append(previousTransaction)
            self.temp[prefixLength] = self.newNamesToOldNames[e]
            
            if utilityPe >= self.minUtil:
                self.highUtilityItemsets.append((utilityPe , self.temp[:prefixLength + 1]))
            
            # caluclate the local utility and subtree utility
            self.useUtilityBinArraysToCalculateUpperBounds(transactionsPe, idx, itemsToKeep)
            newItemsToKeep = []
            newItemsToExplore = []
            for l in range(idx + 1, len(itemsToKeep)):
                itemk = itemsToKeep[l]
                if self.utilityBinArraySU[itemk] >= self.minUtil:
                    newItemsToExplore.append(itemk)
                    newItemsToKeep.append(itemk)
                elif self.utilityBinArrayLU[itemk] >= self.minUtil:
                    newItemsToKeep.append(itemk)
            
            if len(transactionsPe) != 0:
                self.backtrackingEFIM(transactionsPe, newItemsToKeep, newItemsToExplore, prefixLength + 1)

    def useUtilityBinArraysToCalculateUpperBounds(self, transactionsPe, j, itemsToKeep):
        """
            A method to  calculate the sub-tree utility and local utility of all items that can extend itemSet P U {e}
            Attributes:
            -----------
            :param transactionsPe: transactions the projected database for P U {e}
            :type transactionsPe: list
            :param j:he position of j in the list of promising items
            :type j:int
            :param itemsToKeep :the list of promising items
            :type itemsToKeep: list
        """
        for i in range(j + 1, len(itemsToKeep)):
            item = itemsToKeep[i]
            self.utilityBinArrayLU[item] = 0
            self.utilityBinArraySU[item] = 0
        for transaction in transactionsPe:
            sumRemainingUtility = 0
            i = len(transaction.getItems()) - 1
            while i >= transaction.offset:
                item = transaction.getItems()[i]
                if item in itemsToKeep:
                    sumRemainingUtility += transaction.getUtilities()[i]
                    self.utilityBinArraySU[item] += sumRemainingUtility + transaction.prefixUtility
                    self.utilityBinArrayLU[item] += transaction.transactionUtility + transaction.prefixUtility
                i -= 1
                
    def is_equal(self, transaction1, transaction2):
        length1 = len(transaction1.items) - transaction1.offset
        length2 = len(transaction2.items) - transaction2.offset
        if length1 != length2:
            return False
        position1 = transaction1.offset
        position2 = transaction2.offset
        while position1 < len(transaction1.items):
            if transaction1.items[position1] != transaction2.items[position2]:
                return False
            position1 += 1
            position2 += 1
        return True

    def sortDatabase(self, transactions):
        cmp_items = cmp_to_key(self.sort_transaction)
        transactions.sort(key=cmp_items)

    def sort_transaction(self, trans1, trans2):
        trans1_items = trans1.getItems()
        trans2_items = trans2.getItems()
        pos1 = len(trans1_items) - 1
        pos2 = len(trans2_items) - 1
        if len(trans1_items) < len(trans2_items):
            while pos1 >= 0:
                sub = trans2_items[pos2] - trans1_items[pos1]
                if sub != 0:
                    return sub
                pos1 -= 1
                pos2 -= 1
            return -1
        elif len(trans1_items) > len(trans2_items):
            while pos2 >= 0:
                sub = trans2_items[pos2] - trans1_items[pos1]
                if sub != 0:
                    return sub
                pos1 -= 1
                pos2 -= 1
            return 1
        else:
            while pos2 >= 0:
                sub = trans2_items[pos2] - trans1_items[pos1]
                if sub != 0:
                    return sub
                pos1 -= 1
                pos2 -= 1
            return 0
    