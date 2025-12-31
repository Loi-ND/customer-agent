import random
import pandas as pd

class Generator:
    
    def __init__(self, 
                 p_ag_lh: float, 
                 p_ag_mt: float, 
                 p_ag_tv: float,
                 p_ct_lh: float,
                 p_ct_mt: float,
                 p_ct_tv: float,
                 ag_mu: float,
                 ag_sm: float,
                 max_cap: int,
                 num_customers: int,
                 num_agents: int) -> None:
        
        self.p_ag_lh = p_ag_lh
        self.p_ag_mt = p_ag_mt
        self.p_ag_tv = p_ag_tv
        self.p_ct_lh = p_ct_lh
        self.p_ct_mt = p_ct_mt
        self.p_ct_tv = p_ct_tv
        self.ag_mu = ag_mu
        self.ag_sm = ag_sm
        self.max_cap = max_cap
        self.num_customers = num_customers
        self.num_agents = num_agents

    def __gaussian(self):
        while True:
            x = random.gauss(self.ag_mu, self.ag_sm)
            if 0.1 <= x <= 1:
                return x
    
    def generate_customers(self) -> None:

        customers = []

        p1 = self.p_ct_lh
        p2 = self.p_ct_lh + self.p_ct_mt
        for _ in range(self.num_customers):
            id = _
            demand = ""
            p = random.random()
            if p < p1:
                demand = "life_health"
            elif p < p2:
                demand = "motor"
            else:
                demand = "travel"
            customers.append({
                "id": id,
                "demand": demand
            })

        customers = pd.DataFrame(customers)
        customers.to_csv("../data/customers.csv", index=False)
    
    def generate_agents(self) -> None:
        
        agents = []

        for _ in range(self.num_agents):
            id = _

            life_health = False
            motor = False
            travel = False

            while (not life_health) and (not motor) and (not travel):
                p1 = random.random()
                if p1 < self.p_ag_lh:
                    life_health = True
                
                p2 = random.random()
                if p2 < self.p_ag_mt:
                    motor = True
                
                p3 = random.random()
                if p3 < self.p_ag_tv:
                    travel = True
            p = self.__gaussian()
            capacity = int(p*self.max_cap)

            agents.append({
                "id": id,
                "life_health": life_health,
                "motor": motor,
                "travel": travel,
                "capacity": capacity
            })
        
        agents = pd.DataFrame(agents)
        agents.to_csv("../data/agents.csv", index=False)
    