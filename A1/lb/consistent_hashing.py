from sortedcontainers import SortedList

class ConsistentHashMap:
    """
    This class consists of a consistent hash map with support for addition of new server instances and removal of existing server instances.
    
    Attributes:
        num_server_containers: number of server containers
        num_virtual_servers: number of virtual servers per server container
        num_slots: number of slots in the consistent hash map
        server_containers: list of server containers
    """

    def __init__(self, num_server_containers = 3, num_virtual_servers = 9, num_slots = 512) -> None:
        """
        The constructor for ConsistentHashMap class.
        :param num_server_containers: number of server containers
        :param num_virtual_servers: number of virtual servers per server container
        :param num_slots: number of slots in the consistent hash map
        """
        self.num_server_containers = num_server_containers
        self.num_virtual_servers = num_virtual_servers
        self.num_slots = num_slots

        # we create two arrays, one for storing the node themselves, and the other for storing the index of each server in the consistent hash map
        self.sorted_keys = SortedList()
        self.ring = [None] * self.num_slots
        self.server_nodes = [] * self.num_server_containers
        self.server_indexes = {}


    def modulo(self, a: int, b: int) -> int:
        """
        Function implements modulo operation.
        """
        temp = a % b
        if temp < 0:
            temp += b
            temp %= b
        
        return temp


    def virt_serv_hash_func(self, i: int, j: int) -> int:
        """
        This function is used to calculate hash value for a virtual server.
        :param i: server ID
        :param j: virtual server replica ID of server i
        :return: hash value
        """
        i, j = int(i), int(j)
        hash_val = int(i**2 + j**2 + 2*j + 25)
        return hash_val
    

    def req_hash_func(self, i: int) -> int:
        """
        This function is used to calculate hash value for a request.
        :param i: request ID for a client request
        :return: hash value 
        """
        i = int(i)
        hash_val =  int(i**2 + 2*i + 17)
        return hash_val

 
    def __get_empty_slot__(self, hash_val) -> None:
        """
        This function implements quadratic probing to insert a virtual server instance into the consistent hash map.
        :param hash_val: hash value of the virtual server instance
        """
        def probing_func(i: int) -> int:
            return (hash_val + i**2)
        
        for i in range(self.num_slots):
            slot = self.modulo(probing_func(i), self.num_slots)
            
            if self.ring[slot] == None:
                return slot
        
        return -1


    def add_server(self, server_id: int) -> None:
        """
        This function is used to add a new server instance to the consistent hash map.
        :param server_id: ID of the new server instance
        """
        self.server_indexes[server_id] = [None] * self.num_virtual_servers

        for i in range(self.num_virtual_servers):
            temp_hash_val = self.virt_serv_hash_func(server_id, i)
            hash_val = self.modulo(temp_hash_val, self.num_slots)

            slot = self.__get_empty_slot__(hash_val)

            if slot == -1:
                raise Exception("No empty slot found in the consistent hash map.")

            self.sorted_keys.add(slot)
            self.ring[slot] = server_id
            self.server_indexes[server_id][i] = slot


    def remove_server(self, server_id: int) -> None:
        """
        This function is used to remove an existing server instance from the consistent hash map.
        :param server_id: ID of the server instance to be removed
        """
        for i in range(self.num_virtual_servers):
            slot = self.server_indexes[server_id][i]

            self.sorted_keys.remove(slot)
            self.ring[slot] = None
            self.server_indexes[server_id][i] = None
            

    def get_server(self, request_id: int) -> int:
        """
        This function is used to get the server instance to which a request is routed.
        :param request_id: ID of the request
        :return: ID of the server instance to which the request is routed
        """
        hash_val = self.req_hash_func(request_id)
        slot = self.modulo(hash_val, self.num_slots)

        idx = self.sorted_keys.bisect_left(slot)
        if idx == len(self.sorted_keys):
            idx = 0
        
        return self.ring[self.sorted_keys[idx]]