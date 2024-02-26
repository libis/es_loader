
class Array
  def uniqBeginString
    self.uniq!
    array = self
    if array.map { |string| string.class }.uniq === [String]
      array = self.select { |str|
        output = true
        self.each { |el|
          if str != el
            if el.start_with?(str)
              el.start_with?(str)
              output = false
            end
          end
        } 
        output
      }
    end
    array
  end
end


v1  = [
 "1. In één weekend 1500 #vluchtelingen erbij op #Lampedusa! Deze stoet komt dansend #Europa binnen.\n" + "2. #Lesbos zodra er een cameraploeg op het vluchtelingenkamp arriveert, voeren ze een heel scenario op, vrouwen gaan nephuilen en de kinderen helpen ze n handje.\n" + "#ASIELSTOP #EU https://t.co/aFQsd7cXua",
 "1. In één weekend 1500 #vluchtelingen erbij op #Lampedusa! Deze stoet komt dansend #Europa binnen.\n" + "2. #Lesbos zodra er een cameraploeg op het vluchtelingenkamp arriveert, voeren ze een heel scenario op, vrouwen gaan nephuilen en de kinderen helpen ze n handje.\n" + "#ASIELSTOP #EU https://t.co/jScNX4BCac https://t.co/aFQsd7cXua"]






v1  = v1.uniqBeginString
pp "v1v1v1v1v1vv1"
pp v1 



  