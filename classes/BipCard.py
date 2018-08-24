
class BipCard:
    '''
    This class represent the transport card in SCL.
    '''

    def __init__(self, number):
        self.number = number
        self.status = ""
        self.created_on = ""
        self.balance = 0

    def __str__(self):
        return "card_number=%s" % self.number
